#include "ob_hint.h"

using namespace sb;
using namespace sb::common;

namespace sb {
namespace common {
const char* get_consistency_level_str(ObConsistencyLevel level) {
  return (level == WEAK ? "WEAK" : (level == STRONG ? "STRONG" : (level == STATIC ? "STATIC" : ((level == FROZEN) ? "FROZEN" : "NO_CONSISTENCY"))));
}
}
}
