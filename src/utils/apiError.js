// Trích message + details từ rejected action của redux-axios-middleware
export const errInfo = (rejected) => {
  const resp = rejected && rejected.error && rejected.error.response;
  if (resp && resp.data) {
    return {
      status: resp.status,
      message: resp.data.message || "Có lỗi xảy ra",
      details: resp.data.details || null,
      error: resp.data.error,
    };
  }
  return { status: 0, message: "Không kết nối được mock API", details: null };
};
