import { combineReducers } from "redux";
import flink from "./flinkReducer";
import derived from "./derivedReducer";
import alert from "./alertReducer";
import pipeline from "./pipelineReducer";
import explorer from "./explorerReducer";

export default combineReducers({
  flink,
  derived,
  alert,
  pipeline,
  explorer,
});
