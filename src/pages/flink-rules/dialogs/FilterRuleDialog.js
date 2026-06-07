import React from "react";
import { useDispatch } from "react-redux";
import { Form } from "react-final-form";
import { Dialog } from "primereact/dialog";
import { Button } from "primereact/button";
import { TextField, NumberField, DropdownField, required } from "../../../components/FormFields";
import { MODEL_CODE_VALUES } from "../../../mock/devices";
import { createRule, updateRule } from "../../../redux/actions/flinkActions";
import { useToast } from "../../../components/ToastProvider";
import { errInfo } from "../../../utils/apiError";

const MODEL_OPTIONS = MODEL_CODE_VALUES.map((v) => ({ label: v, value: v }));
const ACTION_OPTIONS = [
  { label: "0 - EXCLUDE_IF_MATCH (drop khi TRUE)", value: 0 },
  { label: "1 - INCLUDE_IF_MATCH (chỉ giữ khi TRUE)", value: 1 },
];

const FilterRuleDialog = ({ visible, initial, onHide, onSaved, onPreview }) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const isEdit = !!initial;

  const onSubmit = (values) => {
    const body = {
      vendor_code: values.vendor_code,
      match_path: values.match_path || "",
      match_metric: values.match_metric || null,
      filter_expression: values.filter_expression || "",
      filter_action: Number(values.filter_action),
      priority: Number(values.priority) || 50,
    };
    const req = isEdit
      ? dispatch(updateRule("filterRules", initial.id, body))
      : dispatch(createRule("filterRules", body));
    return req
      .then(() => {
        toast.success("Lưu Filter Rule thành công.");
        onSaved();
        onHide();
      })
      .catch((rej) => {
        const e = errInfo(rej);
        toast.error(e.message);
        return { filter_action: e.message };
      });
  };

  return (
    <Dialog
      header={isEdit ? "Sửa Filter Rule" : "Thêm Filter Rule"}
      visible={visible}
      style={{ width: 680 }}
      onHide={onHide}
      modal
    >
      <Form
        onSubmit={onSubmit}
        initialValues={initial || { vendor_code: "Cisco", filter_action: 0, priority: 50 }}
        render={({ handleSubmit, submitting, values }) => (
          <form onSubmit={handleSubmit}>
            {Number(values.filter_action) === 0 && (
              <div className="warning-box danger">
                <i className="pi pi-exclamation-triangle" /> <strong>EXCLUDE_IF_MATCH:</strong> metric
                khớp sẽ bị <strong>DROP — KHÔNG ghi vào ClickHouse và KHÔNG phục hồi được.</strong> Hãy
                Preview trước khi lưu và kiểm tra priority để rule rộng không đè rule hẹp.
              </div>
            )}
            <DropdownField name="vendor_code" label="Model Code" required options={MODEL_OPTIONS} validate={required()} />
            <DropdownField name="filter_action" label="filter_action" required options={ACTION_OPTIONS} validate={required()} />
            <TextField name="match_path" label="match_path" hint="rỗng = mọi path" placeholder="vd: xr_watchdog_memory" />
            <TextField name="match_metric" label="match_metric" hint="rỗng = mọi metric trong path" placeholder="vd: if_in_discards" />
            <TextField name="filter_expression" label="filter_expression" hint="rỗng = luôn TRUE" placeholder="value < 1000" />
            <NumberField name="priority" label="Priority" hint="rule rộng (priority thấp) không nên đè rule hẹp" />

            <div style={{ textAlign: "right", marginTop: 8 }}>
              {onPreview && (
                <Button
                  label="Preview"
                  icon="pi pi-eye"
                  className="p-button-outlined"
                  type="button"
                  style={{ marginRight: "auto", float: "left" }}
                  onClick={() => onPreview(values)}
                />
              )}
              <Button label="Hủy" className="p-button-text" type="button" onClick={onHide} />
              <Button label="Lưu" icon="pi pi-check" type="submit" loading={submitting} />
            </div>
          </form>
        )}
      />
    </Dialog>
  );
};

export default FilterRuleDialog;
