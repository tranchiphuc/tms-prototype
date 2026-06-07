import axios from "axios";

// ============================================================
// Mock API client
// ------------------------------------------------------------
// Một axios instance với request adapter tùy chỉnh: thay vì gọi
// mạng thật, nó match URL theo các handler đã đăng ký và trả về
// mock data sau một độ trễ giả lập (100–300ms).
//
// Handler được đăng ký từ các module mock (src/mock/handlers/*)
// qua registerHandler(). Mỗi handler nhận { method, url, params,
// data, id } và trả về { status, data } hoặc throw { status, data }
// để mô phỏng lỗi (409/422/404...).
// ============================================================

const handlers = [];

/**
 * Đăng ký một handler.
 * @param {RegExp} pattern  - regex match phần path (sau /api/v1)
 * @param {Function} fn     - (ctx) => { status, data } | Promise
 */
export const registerHandler = (pattern, fn) => {
  handlers.push({ pattern, fn });
};

const BASE = "/api/v1";

const randomDelay = () => 100 + Math.floor(Math.random() * 200);

// Custom adapter — bỏ qua network, gọi handler đã đăng ký
const mockAdapter = (config) => {
  return new Promise((resolve, reject) => {
    const method = (config.method || "get").toLowerCase();
    let url = config.url || "";
    if (url.startsWith(BASE)) url = url.slice(BASE.length);

    // Tách query string khỏi path
    const [path] = url.split("?");

    const params = config.params || {};
    let data = config.data;
    if (typeof data === "string") {
      try {
        data = JSON.parse(data);
      } catch (e) {
        /* giữ nguyên */
      }
    }

    const match = handlers.find((h) => h.pattern.test(path));

    setTimeout(async () => {
      if (!match) {
        // eslint-disable-next-line no-console
        console.warn(`[mockApi] Không có handler cho: ${method.toUpperCase()} ${path}`);
        return reject({
          response: { status: 404, data: { message: `No mock handler: ${path}` } },
          config,
        });
      }

      const m = match.pattern.exec(path);
      const ctx = {
        method,
        path,
        params,
        data,
        match: m, // capture groups (m[1] = id nếu pattern có group)
        id: m && m[1] ? m[1] : undefined,
      };

      try {
        const result = await match.fn(ctx);
        const status = (result && result.status) || 200;
        resolve({
          data: result ? result.data : null,
          status,
          statusText: "OK",
          headers: {},
          config,
        });
      } catch (err) {
        // err = { status, data } để mô phỏng lỗi HTTP
        const status = (err && err.status) || 500;
        reject({
          response: { status, data: (err && err.data) || { message: "Mock error" } },
          config,
        });
      }
    }, randomDelay());
  });
};

const client = axios.create({
  baseURL: BASE,
  adapter: mockAdapter,
});

export default client;
