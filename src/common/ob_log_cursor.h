#ifndef __OCEANBASE_COMMON_OB_LOG_CURSOR_H__
#define __OCEANBASE_COMMON_OB_LOG_CURSOR_H__
#include "ob_define.h"

namespace sb {
namespace common {
struct ObLogCursor {
  int64_t file_id_;
  int64_t log_id_;
  int64_t offset_;
  ObLogCursor(): file_id_(0), log_id_(0), offset_(0)
  {}
  ~ObLogCursor()
  {}
  bool is_valid() {
    return file_id_ >= 0 && log_id_ >= 0 && offset_ >= 0;
  }
  int serialize(char* buf, int64_t len, int64_t& pos) const {
    int rc = OB_SUCCESS;
    if (!(OB_SUCCESS == serialization::encode_i64(buf, len, pos, file_id_)
          && OB_SUCCESS == serialization::encode_i64(buf, len, pos, log_id_)
          && OB_SUCCESS == serialization::encode_i64(buf, len, pos, offset_))) {
      rc = OB_SERIALIZE_ERROR;
      TBSYS_LOG(WARN, "ObLogSyncPoint.serialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, pos, rc);
    }
    return rc;
  }

  int deserialize(char* buf, int64_t len, int64_t& pos) const {
    int rc = OB_SUCCESS;
    if (!(OB_SUCCESS == serialization::decode_i64(buf, len, pos, (int64_t*)&file_id_)
          && OB_SUCCESS == serialization::decode_i64(buf, len, pos, (int64_t*)&log_id_)
          && OB_SUCCESS == serialization::decode_i64(buf, len, pos, (int64_t*)&offset_))) {
      rc = OB_DESERIALIZE_ERROR;
      TBSYS_LOG(WARN, "ObLogSyncPoint.deserialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, pos, rc);
    }
    return rc;
  }
  char* to_str() {
    static char buf[512];
    snprintf(buf, sizeof(buf), "ObLogCursor{file_id=%ld, log_id=%ld, offset=%ld}", file_id_, log_id_, offset_);
    buf[sizeof(buf) - 1] = 0;
    return buf;
  }
};
}; // end namespace common
}; // end namespace sb
#endif // __OCEANBASE_COMMON_OB_LOG_CURSOR_H__
