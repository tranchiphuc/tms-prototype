// ============================================================
// Mock handlers registry
// ------------------------------------------------------------
// Mỗi phase import module handler tương ứng vào đây để side-effect
// registerHandler() chạy. Phase 0 chưa có handler nào — sẽ bổ sung:
//   import "./derivedHandlers";  // Phase 2
//   import "./alertHandlers";    // Phase 3
//   import "./pipelineHandlers"; // Phase 4
//   import "./explorerHandlers"; // Phase 5
// ============================================================

import "./flinkHandlers"; // Phase 1
import "./derivedHandlers"; // Phase 2
import "./alertHandlers"; // Phase 3

export {};
