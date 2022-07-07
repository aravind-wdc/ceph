// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>
#include <linux/blkzoned.h>

#include "crimson/os/seastore/segment_manager/zns.h"
#include "crimson/common/config_proxy.h"
#include "crimson/os/seastore/logging.h"
#include "include/buffer.h"

SET_SUBSYS(seastore_device);

#define SECT_SHIFT 9
namespace crimson::os::seastore::segment_manager::zns {

using open_device_ret = ZNSSegmentManager::access_ertr::future<
  std::pair<seastar::file, seastar::stat_data>>;
static open_device_ret open_device(
  const std::string &path,
  seastar::open_flags mode)
{
  LOG_PREFIX(ZNSSegmentManager::open_device);
  return seastar::file_stat(
    path, seastar::follow_symlink::yes
  ).then([FNAME, mode, &path](auto stat) mutable{
    return seastar::open_file_dma(path, mode).then([=](auto file){
       DEBUG("open of device {} successful, size {}",
        path,
        stat.size);
      return std::make_pair(file, stat);
    });
  }).handle_exception(
    [FNAME](auto e) -> open_device_ret {
	ERROR("got error {}",
	e);
      return crimson::ct_error::input_output_error::make();
    }
  );
}

static zns_sm_metadata_t make_metadata(
  seastore_meta_t meta,
  const seastar::stat_data &data,
  size_t zone_size,
  size_t zone_capacity,
  size_t num_zones)
{
  LOG_PREFIX(ZNSSegmentManager::make_metadata);
  using crimson::common::get_conf;
  
  auto config_size = get_conf<Option::size_t>(
    "seastore_device_size");
  
  size_t size = (data.size == 0) ? config_size : data.size;
  
  auto config_segment_size = get_conf<Option::size_t>(
    "seastore_segment_size");

  size_t segment_size = (zone_size == 0) ? config_segment_size : (zone_size << SECT_SHIFT);
  size_t zones_per_segment = segment_size / (zone_size << SECT_SHIFT);
  size_t segments = (num_zones - 1) * zones_per_segment;
  
  INFO(
    "\n\t device size {}, block_size {}, allocated_size {}, config_size {},"
    "\n\t config_segment_size {}, total zones {}, zone_size {}, zone_capacity {},"
    "\n\t total segments {}, zones per segment {}, segment size {}",
    size,
    data.block_size,
    data.allocated_size,
    config_size,
    config_segment_size,
    num_zones,
    zone_size << SECT_SHIFT,
    zone_capacity << SECT_SHIFT,
    segments,
    zones_per_segment,
    (zone_capacity << SECT_SHIFT) * zones_per_segment);
  
  zns_sm_metadata_t ret = zns_sm_metadata_t{
    size,
    segment_size,
    zone_capacity * zones_per_segment,
    zones_per_segment,
    zone_capacity,
    data.block_size,
    segments,
    zone_size,
    zone_size,
    meta};
  return ret;
}

struct ZoneReport {
  struct blk_zone_report *hdr;
  ZoneReport(int nr_zones) 
    : hdr((blk_zone_report *)malloc(
	    sizeof(struct blk_zone_report) + nr_zones * sizeof(struct blk_zone))){;}
  ~ZoneReport(){
    free(hdr);
  }
  ZoneReport(const ZoneReport &) = delete;
  ZoneReport(ZoneReport &&rhs) : hdr(rhs.hdr) {
    rhs.hdr = nullptr;
  }
};

static seastar::future<> reset_device(
  seastar::file &device, 
  uint32_t zone_size, 
  uint32_t nr_zones)
{
  return seastar::do_with(
    blk_zone_range{},
    ZoneReport(nr_zones),
    [&, nr_zones] (auto &range, auto &zr){
      range.sector = 0;
      range.nr_sectors = zone_size * nr_zones;
      return device.ioctl(
	BLKRESETZONE, 
	&range
      ).then([&](int ret){
	return seastar::now();
      });
    }
  );
}

static seastar::future<size_t> get_zone_capacity(
  seastar::file &device, 
  uint32_t zone_size, 
  uint32_t nr_zones)
{
  return seastar::do_with(
    blk_zone_range{},
    ZoneReport(nr_zones),
    [&] (auto &first_zone_range, auto &zr){
      first_zone_range.sector = 0;
      first_zone_range.nr_sectors = zone_size;
      return device.ioctl(
	BLKOPENZONE, 
	&first_zone_range
      ).then([&](int ret){
        zr.hdr->sector = 0;
        zr.hdr->nr_zones = nr_zones;
	return device.ioctl(BLKREPORTZONE, zr.hdr);
      }).then([&] (int ret){
	return device.ioctl(BLKRESETZONE, &first_zone_range);
      }).then([&](int ret){
	return seastar::make_ready_future<size_t>(zr.hdr->zones[0].capacity);
      });
    }
  );
}

static write_ertr::future<> do_write(
  seastar::file &device,
  uint64_t offset,
  bufferptr &bptr)
{
  LOG_PREFIX(ZNSSegmentManager::do_write);
  DEBUG(
    "offset {} len {}",
    offset,
    bptr.length());
  return device.dma_write(
    offset,
    bptr.c_str(),
    bptr.length() 
  ).handle_exception(
    [FNAME](auto e) -> write_ertr::future<size_t> {
      ERROR(
        "dma_write got error {}",
        e);
      return crimson::ct_error::input_output_error::make();
    }
  ).then([length = bptr.length()](auto result) -> write_ertr::future<> {
    if (result != length) {
      return crimson::ct_error::input_output_error::make();
    }
    return write_ertr::now();
  });
}

static write_ertr::future<> do_writev(
  seastar::file &device,
  uint64_t offset,
  bufferlist&& bl,
  size_t block_size)
{
  LOG_PREFIX(ZNSSegmentManager::do_writev);
  DEBUG("offset {} len {}",
    offset,
    bl.length());
  // writev requires each buffer to be aligned to the disks' block
  // size, we need to rebuild here
  bl.rebuild_aligned(block_size);
  
  std::vector<iovec> iov;
  bl.prepare_iov(&iov);
  return device.dma_write(
    offset,
    std::move(iov)
  ).handle_exception(
    [FNAME](auto e) -> write_ertr::future<size_t> {
      ERROR("dma_write got error {}",
	e);
      return crimson::ct_error::input_output_error::make();
    }
  ).then([bl=std::move(bl)/* hold the buf until the end of io */](size_t written)
	 -> write_ertr::future<> {
    if (written != bl.length()) {
      return crimson::ct_error::input_output_error::make();
    }
    return write_ertr::now();
  });
}

static ZNSSegmentManager::access_ertr::future<>
write_metadata(seastar::file &device, zns_sm_metadata_t sb)
{
  assert(ceph::encoded_sizeof_bounded<zns_sm_metadata_t>() <
	 sb.block_size);
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(sb.block_size)),
    [=, &device](auto &bp){
      LOG_PREFIX(ZNSSegmentManager::write_metadata);
      DEBUG("block_size {}", sb.block_size);
      bufferlist bl;
      encode(sb, bl);
      auto iter = bl.begin();
      assert(bl.length() < sb.block_size);
      DEBUG("buffer length {}", bl.length());
      iter.copy(bl.length(), bp.c_str());
      DEBUG("doing writeout");
      return do_write(device, 0, bp);
    });
}

static read_ertr::future<> do_read(
  seastar::file &device,
  uint64_t offset,
  size_t len,
  bufferptr &bptr)
{
  LOG_PREFIX(ZNSSegmentManager::do_read);
  assert(len <= bptr.length());
  DEBUG("offset {} len {}",
    offset,
    len);
  return device.dma_read(
    offset,
    bptr.c_str(),
    len
  ).handle_exception(
    [FNAME](auto e) -> read_ertr::future<size_t> {
      ERROR("dma_read got error {}",
        e);
      return crimson::ct_error::input_output_error::make();
    }
  ).then([len](auto result) -> read_ertr::future<> {
    if (result != len) {
      return crimson::ct_error::input_output_error::make();
    }
    return read_ertr::now();
  });
}

static
ZNSSegmentManager::access_ertr::future<zns_sm_metadata_t>
read_metadata(seastar::file &device, seastar::stat_data sd)
{
  assert(ceph::encoded_sizeof_bounded<zns_sm_metadata_t>() <
	 sd.block_size);
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(sd.block_size)),
    [=, &device](auto &bp) {
      return do_read(
	device,
	0,
	bp.length(),
	bp
      ).safe_then([=, &bp] {
	bufferlist bl;
	bl.push_back(bp);
	zns_sm_metadata_t ret;
	auto bliter = bl.cbegin();
	decode(ret, bliter);
	return ZNSSegmentManager::access_ertr::future<zns_sm_metadata_t>(
	  ZNSSegmentManager::access_ertr::ready_future_marker{},
	  ret);
      });
    });
}

ZNSSegmentManager::mount_ret ZNSSegmentManager::mount() 
{
  return open_device(
    device_path, seastar::open_flags::rw
  ).safe_then([=](auto p) {
    device = std::move(p.first);
    auto sd = p.second;
    return read_metadata(device, sd);
  }).safe_then([=](auto meta){
    metadata = meta;
    return mount_ertr::now();
  });
}

ZNSSegmentManager::mkfs_ret ZNSSegmentManager::mkfs(
  device_config_t config)
{
  LOG_PREFIX(ZNSSegmentManager::mkfs);
  INFO("starting, device_path {}", device_path);
  return seastar::do_with(
    seastar::file{},
    seastar::stat_data{},
    zns_sm_metadata_t{},
    size_t(),
    size_t(),
    [=](auto &device, auto &stat, auto &sb, auto &zone_size, auto &nr_zones){
      return open_device(
	device_path, 
	seastar::open_flags::rw
      ).safe_then([=, &device, &stat, &sb, &zone_size, &nr_zones](auto p){
	device = p.first;
	stat = p.second;
	return device.ioctl(
	  BLKGETNRZONES, 
	  (void *)&nr_zones
	).then([&](int ret){
	  if (nr_zones == 0) {
	    return seastar::make_exception_future<int>(
	      std::system_error(std::make_error_code(std::errc::io_error)));
	  }
	  return device.ioctl(BLKGETZONESZ, (void *)&zone_size);
	}).then([&] (int ret){
	  return reset_device(device, zone_size, nr_zones);
	}).then([&] {
	  return get_zone_capacity(device, zone_size, nr_zones); 
	}).then([&, FNAME, config] (auto zone_capacity){
	  sb = make_metadata(
	    config.meta, 
	    stat, 
	    zone_size, 
	    zone_capacity, 
	    nr_zones);
	  metadata = sb;
	  stats.metadata_write.increment(
	    ceph::encoded_sizeof_bounded<zns_sm_metadata_t>());
	  DEBUG("Wrote to stats.");
	  return write_metadata(device, sb);
	}).finally([&, FNAME] {
	  DEBUG("Closing device.");
	  return device.close(); 
	}).safe_then([FNAME] {
	  DEBUG("Returning from mkfs.");
	  return mkfs_ertr::now();
	});
      });
    });
}

struct blk_zone_range make_range(
  segment_id_t id, 
  size_t segment_size, 
  size_t block_size, 
  size_t first_segment_offset)
{
  return blk_zone_range{
    (id.device_segment_id() * segment_size + first_segment_offset),
    (segment_size)  
  };
}

using blk_open_zone_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;
using blk_open_zone_ret = blk_open_zone_ertr::future<>;
blk_open_zone_ret blk_open_zone(seastar::file &device, blk_zone_range &range){
  return device.ioctl(
    BLKOPENZONE, 
    &range
  ).then_wrapped([=](auto f) -> blk_open_zone_ret{
    if (f.failed()) {
      return crimson::ct_error::input_output_error::make();
    }
    else {
      int ret = f.get();
      if (ret == 0) {
	return seastar::now();
      } else {
	return crimson::ct_error::input_output_error::make();
      }
    }
  });
}

ZNSSegmentManager::open_ertr::future<SegmentRef> ZNSSegmentManager::open(
  segment_id_t id)
{
  LOG_PREFIX(ZNSSegmentManager::open);
  return seastar::do_with(
    blk_zone_range{},
    [=] (auto &range){
      range = make_range(
	id, 
	metadata.zone_size, 
	metadata.block_size, 
	metadata.first_segment_offset);
      return blk_open_zone(
	device, 
	range
      );
    }
  ).safe_then([=] {
    DEBUG("segment {}, open successful", id);
    return open_ertr::future<SegmentRef>(
      open_ertr::ready_future_marker{},
      SegmentRef(new ZNSSegment(*this, id))
    );
  });
}

using blk_close_zone_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;
using blk_close_zone_ret = blk_close_zone_ertr::future<>;
blk_close_zone_ret blk_close_zone(
  seastar::file &device, 
  blk_zone_range &range)
{
  return device.ioctl(
    //BLKCLOSEZONE, 
    BLKFINISHZONE,
    &range
  ).then_wrapped([=](auto f) -> blk_open_zone_ret{
    if (f.failed()) {
      return crimson::ct_error::input_output_error::make();
    }
    else {
      int ret = f.get();
      if (ret == 0) {
	return seastar::now();
      } else {
	return crimson::ct_error::input_output_error::make();
      }
    }
  });
}

ZNSSegmentManager::release_ertr::future<> ZNSSegmentManager::release(
  segment_id_t id) 
{
  LOG_PREFIX(ZNSSegmentManager::release);
  return seastar::do_with(
    blk_zone_range{},
    [=] (auto &range){
      range = make_range(
	id, 
	metadata.zone_size, 
	metadata.block_size, 
	metadata.first_segment_offset);
      return blk_close_zone(
	device, 
	range
      );
    }
  ).safe_then([=] {
    DEBUG("segment release successful");
    return release_ertr::now();
  });
}

SegmentManager::read_ertr::future<> ZNSSegmentManager::read(
  paddr_t addr,
  size_t len,
  ceph::bufferptr &out)
{
  LOG_PREFIX(ZNSSegmentManager::read);
  auto& seg_addr = addr.as_seg_paddr();
  if (seg_addr.get_segment_id().device_segment_id() >= get_num_segments()) {
    ERROR("invalid segment {}",
      addr);
    return crimson::ct_error::invarg::make();
  }
  
  if (seg_addr.get_segment_off() + len > (metadata.zone_size << SECT_SHIFT)) {
    ERROR("invalid offset {}, len {} get_segment_off ={}, zone_size = {}, zone-size in bytes = {}",
      addr,
      len,
      seg_addr.get_segment_off(),
      metadata.zone_size,
      metadata.zone_size << SECT_SHIFT);
    return crimson::ct_error::invarg::make();
  }
  return do_read(
    device,
    get_offset(addr),
    len,
    out);
}

Segment::close_ertr::future<> ZNSSegmentManager::segment_close(
  segment_id_t id, seastore_off_t write_pointer)
{
  LOG_PREFIX(ZNSSegmentManager::close);
  return seastar::do_with(
    blk_zone_range{},
    [=] (auto &range){
      range = make_range(
	id, 
	metadata.zone_size, 
	metadata.block_size, 
	metadata.first_segment_offset);
      //return blk_finish_zone(
      return blk_close_zone(
	device, 
	range
      );
    }
  ).safe_then([=] {
    DEBUG("close successful");
    return Segment::close_ertr::now();
  });
}

Segment::write_ertr::future<> ZNSSegmentManager::segment_write(
  paddr_t addr,
  ceph::bufferlist bl,
  bool ignore_check)
{
  LOG_PREFIX(ZNSSegmentManager::segment_write);
  assert(addr.get_device_id() == get_device_id());
  assert((bl.length() % metadata.block_size) == 0);
  auto& seg_addr = addr.as_seg_paddr();
  DEBUG("write to segment {} at offset {}, physical offset {}, len {}",
    seg_addr.get_segment_id(),
    seg_addr.get_segment_off(),
    get_offset(addr),
    bl.length());
  stats.data_write.increment(bl.length());
  return do_writev(
    device, 
    get_offset(addr), 
    std::move(bl), 
    metadata.block_size);
}

device_id_t ZNSSegmentManager::get_device_id() const
{
  return metadata.device_id;
};

secondary_device_set_t& ZNSSegmentManager::get_secondary_devices()
{
  return metadata.secondary_devices;
};

magic_t ZNSSegmentManager::get_magic() const
{
  return metadata.magic;
};

seastore_off_t ZNSSegment::get_write_capacity() const
{
  return manager.get_segment_size();
}

SegmentManager::close_ertr::future<> ZNSSegmentManager::close()
{
  if (device) {
    return device.close();
  }
  return seastar::now();
}

Segment::close_ertr::future<> ZNSSegment::close()
{
  return manager.segment_close(id, write_pointer);
}

Segment::write_ertr::future<> ZNSSegment::write(
  seastore_off_t offset, ceph::bufferlist bl)
{
  LOG_PREFIX(ZNSSegment::write);
  DEBUG("arav: Write to segment {} at wp {} offset {} len {}", id, write_pointer, offset, bl.length());
  if (offset != write_pointer || offset % manager.metadata.block_size != 0) {
    ERROR("Invalid segment write on segment {} to offset {}",
      id,
      offset);
    return crimson::ct_error::invarg::make();
  }
  if (offset + bl.length() > manager.metadata.segment_size)
    return crimson::ct_error::enospc::make();
  
  write_pointer = offset + bl.length();
  return manager.segment_write(paddr_t::make_seg_paddr(id, offset), bl);
}

Segment::write_ertr::future<> ZNSSegment::write_padd(
  seastore_off_t offset, unsigned int &n_wr_req, size_t padd_bytes)
{
  LOG_PREFIX(ZNSSegment::write_padd);

  DEBUG("Write to segment {} at wp {}", id, write_pointer);
  DEBUG("arav: wp = {} seg_total_size= {}, seg-slba={} n_wr_req = {}",
        write_pointer, manager.metadata.segment_size, id.device_segment_id() * manager.metadata.segment_size, n_wr_req);
  

  
  //n_wr_req++;
  return crimson::repeat([&n_wr_req, FNAME, this] {
    bufferptr bp(ceph::buffer::create_page_aligned(4096));
    bp.zero();
    bufferlist padd_bl;
    padd_bl.append(bp);
    return write(write_pointer, padd_bl).safe_then([&n_wr_req, FNAME, this] () {
      //n_wr_req--;
      DEBUG("3Write complete. next write to segment {} at wp {} ", id, write_pointer);
      if (write_pointer == manager.metadata.segment_size - (size_t)4096)
        return write_ertr::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
      else
        return write_ertr::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
    }).handle_error(
     write_ertr::pass_further{},
     crimson::ct_error::assert_all{
       "Write error in ZNSSegment::write_padd"
     });
    }).safe_then([] () {
      return write_ertr::now();
    }).handle_error(
     write_ertr::pass_further{},
     crimson::ct_error::assert_all{
       "Repeat error in ZNSSegment::write_padd"
     });
}

Segment::write_ertr::future<> ZNSSegment::write_segment_tail(
  seastore_off_t offset, ceph::bufferlist bl)
{
  LOG_PREFIX(ZNSSegment::write_segment_tail);
  DEBUG("Write to segment {} at wp {}", id, write_pointer);
  DEBUG("arav: wp = {} seg_total_size= {}, tail-size={}, seg-slba={} padding size = {}",
        write_pointer, manager.metadata.segment_size, bl.length(), id.device_segment_id() * manager.metadata.segment_size, manager.metadata.segment_size - write_pointer - bl.length());
  
  if (write_pointer + bl.length() > manager.metadata.segment_size)
    return crimson::ct_error::enospc::make();

  size_t padding_bytes = manager.metadata.segment_size - write_pointer - bl.length();
  assert(padding_bytes % 4096 == 0);
  unsigned int n_wr_req = padding_bytes / 4096;

  return write_padd(write_pointer, n_wr_req, padding_bytes).safe_then([=] () {
  DEBUG("Completed padding, Write tail to segment {} at offset {} , wp {}", id, offset, write_pointer);
  return write(offset, bl).safe_then([FNAME, this] () {
      DEBUG("tail info write complete to segment {} curr wp at {}", id, write_pointer);
      return write_ertr::now();
     }).handle_error(
     write_ertr::pass_further{},
     crimson::ct_error::assert_all{
       "Invalid error in ZNSSegment::write_segment_tail"
     });
    });

}

}
