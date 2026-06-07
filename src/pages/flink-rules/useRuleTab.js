import { useState, useCallback } from "react";
import { useDispatch } from "react-redux";
import { confirmDialog } from "primereact/confirmdialog";
import { deleteRule } from "../../redux/actions/flinkActions";
import { useToast } from "../../components/ToastProvider";
import { errInfo } from "../../utils/apiError";

// Logic dùng chung cho mỗi tab Flink Rule.
export const useRuleTab = (slice) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const [reloadToken, setReloadToken] = useState(0);
  const [editing, setEditing] = useState(null);
  const [dialogVisible, setDialogVisible] = useState(false);
  const [previewRow, setPreviewRow] = useState(null);

  const reload = useCallback(() => setReloadToken((t) => t + 1), []);

  const openAdd = useCallback(() => {
    setEditing(null);
    setDialogVisible(true);
  }, []);

  const openEdit = useCallback((row) => {
    setEditing(row);
    setDialogVisible(true);
  }, []);

  const closeDialog = useCallback(() => setDialogVisible(false), []);

  const openPreview = useCallback((row) => setPreviewRow(row), []);
  const closePreview = useCallback(() => setPreviewRow(null), []);

  const doDelete = useCallback(
    (row, force) =>
      dispatch(deleteRule(slice, row.id, force))
        .then(() => {
          toast.success("Đã Deprecate (soft-delete) bản ghi.");
          reload();
        })
        .catch((rej) => {
          const e = errInfo(rej);
          if (e.status === 409 && Array.isArray(e.details) && e.details.length) {
            // Cascade — hỏi force
            const list = e.details.map((c) => `• ${c.group} #${c.id} (${c.name})`).join("\n");
            confirmDialog({
              header: "Cảnh báo cascade",
              message: `${e.message}\n\nCác rule con Active bị ảnh hưởng:\n${list}\n\nDeprecate luôn các con?`,
              icon: "pi pi-exclamation-triangle",
              acceptClassName: "p-button-danger",
              acceptLabel: "Force cascade",
              rejectLabel: "Hủy",
              accept: () => doDelete(row, true),
            });
          } else {
            toast.error(e.message);
          }
        }),
    [dispatch, slice, toast, reload]
  );

  const handleDelete = useCallback(
    (row) => {
      confirmDialog({
        header: "Xác nhận Deprecate",
        message: `Deprecate bản ghi #${row.id}? (soft-delete: status=0)`,
        icon: "pi pi-info-circle",
        acceptLabel: "Deprecate",
        rejectLabel: "Hủy",
        accept: () => doDelete(row, false),
      });
    },
    [doDelete]
  );

  return {
    reloadToken,
    reload,
    editing,
    dialogVisible,
    openAdd,
    openEdit,
    closeDialog,
    handleDelete,
    previewRow,
    openPreview,
    closePreview,
  };
};
