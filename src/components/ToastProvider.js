import React, { createContext, useContext, useRef, useCallback } from "react";
import { Toast } from "primereact/toast";

const ToastContext = createContext(null);

export const ToastProvider = ({ children }) => {
  const ref = useRef(null);

  const show = useCallback((opts) => {
    if (ref.current) ref.current.show(opts);
  }, []);

  const success = useCallback(
    (detail, summary = "Thành công") => show({ severity: "success", summary, detail, life: 3000 }),
    [show]
  );
  const error = useCallback(
    (detail, summary = "Lỗi") => show({ severity: "error", summary, detail, life: 5000 }),
    [show]
  );
  const warn = useCallback(
    (detail, summary = "Cảnh báo") => show({ severity: "warn", summary, detail, life: 4000 }),
    [show]
  );

  return (
    <ToastContext.Provider value={{ show, success, error, warn }}>
      <Toast ref={ref} position="top-right" />
      {children}
    </ToastContext.Provider>
  );
};

export const useToast = () => {
  const ctx = useContext(ToastContext);
  return ctx || { show: () => {}, success: () => {}, error: () => {}, warn: () => {} };
};
