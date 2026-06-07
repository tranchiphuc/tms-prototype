import { createStore, applyMiddleware, compose } from "redux";
import axiosMiddleware from "redux-axios-middleware";
import rootReducer from "./reducers";
import client from "../services/mockApi";

// Đăng ký toàn bộ mock handler (side-effect import)
import "../mock/handlers";

const composeEnhancers =
  (typeof window !== "undefined" && window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__) || compose;

const store = createStore(
  rootReducer,
  composeEnhancers(
    applyMiddleware(
      axiosMiddleware(client, {
        // Cho phép .catch() bắt lỗi (409/422/...) ở component
        returnRejectedPromiseOnError: true,
      })
    )
  )
);

export default store;
