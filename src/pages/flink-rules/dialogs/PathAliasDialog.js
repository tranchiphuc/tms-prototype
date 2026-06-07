import React from "react";
import { useDispatch } from "react-redux";
import { Form } from "react-final-form";
import { Dialog } from "primereact/dialog";
import { Button } from "primereact/button";
import { TextField, TextAreaField, DropdownField, required } from "../../../components/FormFields";
import { MODEL_CODE_VALUES } from "../../../mock/devices";
import { createRule, updateRule } from "../../../redux/actions/flinkActions";
import { useToast } from "../../../components/ToastProvider";
import { errInfo } from "../../../utils/apiError";

const MODEL_OPTIONS = MODEL_CODE_VALUES.map((v) => ({ label: v, value: v }));

const PathAliasDialog = ({ visible, initial, onHide, onSaved }) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const isEdit = !!initial;
  const aliasLocked = isEdit && Number(initial.status) === 1;

  const onSubmit = (values) => {
    const body = {
      vendor_code: values.vendor_code,
      original_path: values.original_path,
      alias_path: values.alias_path,
    };
    const req = isEdit
      ? dispatch(updateRule("pathAliases", initial.id, body))
      : dispatch(createRule("pathAliases", body));
    return req
      .then(() => {
        toast.success("Lưu thành công. Có hiệu lực sau ~1 chu kỳ refresh Flink.");
        onSaved();
        onHide();
      })
      .catch((rej) => {
        const e = errInfo(rej);
        toast.error(e.message);
        return { [e.details?.[0]?.field || "original_path"]: e.message };
      });
  };

  return (
    <Dialog
      header={isEdit ? "Sửa Path Alias" : "Thêm Path Alias"}
      visible={visible}
      style={{ width: 640 }}
      onHide={onHide}
      modal
    >
      <Form
        onSubmit={onSubmit}
        initialValues={initial || { vendor_code: "Cisco" }}
        render={({ handleSubmit, submitting, values }) => (
          <form onSubmit={handleSubmit}>
            <DropdownField
              name="vendor_code"
              label="Model Code"
              required
              options={MODEL_OPTIONS}
              validate={required()}
              hint="Data model của path (Cisco/Juniper/Nokia/OpenConfig/All)"
            />
            <TextAreaField
              name="original_path"
              label="Original Path (YANG)"
              required
              rows={2}
              validate={required()}
              placeholder="Cisco-IOS-XR-...:.../..."
            />
            {values.vendor_code === "OpenConfig" &&
              values.original_path &&
              !values.original_path.startsWith("openconfig-") && (
                <div className="warning-box">
                  Model Code = OpenConfig nhưng path không bắt đầu bằng <code>openconfig-</code> — rule
                  sẽ không bao giờ match.
                </div>
              )}
            <TextField
              name="alias_path"
              label="Alias Path"
              required
              disabled={aliasLocked}
              validate={required()}
              placeholder="vd: xr_watchdog_memory"
              hint={
                aliasLocked
                  ? "⚠ alias_path KHÔNG sửa được sau khi Active — Deprecate row cũ + tạo mới."
                  : undefined
              }
            />
            {aliasLocked && (
              <div className="warning-box">
                Bản ghi đang Active: chỉ sửa được Original Path / Model Code. Để đổi Alias Path, hãy
                Deprecate bản ghi này và tạo bản ghi mới (cảnh báo cascade metric/label alias con).
              </div>
            )}
            <div style={{ textAlign: "right", marginTop: 8 }}>
              <Button label="Hủy" className="p-button-text" type="button" onClick={onHide} />
              <Button label="Lưu" icon="pi pi-check" type="submit" loading={submitting} />
            </div>
          </form>
        )}
      />
    </Dialog>
  );
};

export default PathAliasDialog;
