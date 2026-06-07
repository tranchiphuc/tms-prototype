import React, { useEffect, useState } from "react";
import { useDispatch, useSelector } from "react-redux";
import { Form } from "react-final-form";
import { Dialog } from "primereact/dialog";
import { Button } from "primereact/button";
import { InputText } from "primereact/inputtext";
import {
  TextField,
  TextAreaField,
  NumberField,
  DropdownField,
  required,
} from "../../../components/FormFields";
import { MODEL_CODE_VALUES } from "../../../mock/devices";
import { createRule, updateRule, fetchList } from "../../../redux/actions/flinkActions";
import { useToast } from "../../../components/ToastProvider";
import { errInfo } from "../../../utils/apiError";

const MODEL_OPTIONS = MODEL_CODE_VALUES.map((v) => ({ label: v, value: v }));
const LV_OPTIONS = [
  { label: "0 - identity", value: 0 },
  { label: "1 - regex", value: 1 },
  { label: "2 - enum_mapping", value: 2 },
];

// Preview realtime transform (FR-04): nhập value mẫu → value sau transform
const previewTransform = (lvKind, sample, values) => {
  if (!sample) return "";
  try {
    if (Number(lvKind) === 1 && values.lv_pattern) {
      const re = new RegExp(values.lv_pattern);
      return sample.replace(re, values.lv_replace || "");
    }
    if (Number(lvKind) === 2 && values.lv_mapping_text) {
      const map = JSON.parse(values.lv_mapping_text || "{}");
      return map[sample] !== undefined ? map[sample] : "(không khớp mapping)";
    }
    return sample; // identity
  } catch (e) {
    return "(lỗi pattern/mapping)";
  }
};

const LabelAliasDialog = ({ visible, initial, onHide, onSaved }) => {
  const dispatch = useDispatch();
  const toast = useToast();
  const isEdit = !!initial;
  const pathList = useSelector((s) => s.flink.pathAliases.list);
  const [sample, setSample] = useState("");

  useEffect(() => {
    if (visible && pathList.length === 0) {
      dispatch(fetchList("pathAliases", { page_size: 200, status: "" }));
    }
  }, [visible, pathList.length, dispatch]);

  const pathOptions = [{ label: "(any-path · null)", value: null }].concat(
    pathList.filter((p) => Number(p.status) === 1).map((p) => ({ label: `${p.alias_path} (#${p.id})`, value: p.id }))
  );

  const prepare = (v) =>
    !v
      ? { vendor_code: "Cisco", path_alias_id: null, lv_kind: 0, priority: 100 }
      : { ...v, lv_mapping_text: v.lv_mapping ? JSON.stringify(v.lv_mapping, null, 2) : "" };

  const onSubmit = (values) => {
    const lk = Number(values.lv_kind);
    let lv_mapping = null;
    if (lk === 2) {
      try {
        lv_mapping = JSON.parse(values.lv_mapping_text || "{}");
      } catch (e) {
        toast.error("lv_mapping không phải JSON hợp lệ");
        return { lv_mapping_text: "JSON không hợp lệ" };
      }
    }
    const body = {
      vendor_code: values.vendor_code,
      path_alias_id: values.path_alias_id ?? null,
      original_key: values.original_key,
      alias_key: values.alias_key,
      lv_kind: lk,
      lv_pattern: lk === 1 ? values.lv_pattern : null,
      lv_replace: lk === 1 ? values.lv_replace : null,
      lv_mapping,
      priority: Number(values.priority) || 50,
    };
    const req = isEdit
      ? dispatch(updateRule("labelAliases", initial.id, body))
      : dispatch(createRule("labelAliases", body));
    return req
      .then(() => {
        toast.success("Lưu Label Alias thành công.");
        onSaved();
        onHide();
      })
      .catch((rej) => {
        const e = errInfo(rej);
        toast.error(e.message);
        return { [e.details?.[0]?.field || "alias_key"]: e.message };
      });
  };

  return (
    <Dialog
      header={isEdit ? "Sửa Label Alias" : "Thêm Label Alias"}
      visible={visible}
      style={{ width: 680 }}
      onHide={onHide}
      modal
    >
      <Form
        onSubmit={onSubmit}
        initialValues={prepare(initial)}
        render={({ handleSubmit, submitting, values }) => {
          const lk = Number(values.lv_kind);
          return (
            <form onSubmit={handleSubmit}>
              <DropdownField name="vendor_code" label="Model Code" required options={MODEL_OPTIONS} validate={required()} />
              <DropdownField name="path_alias_id" label="Path scope" options={pathOptions} />
              <TextField name="original_key" label="Original Key" required validate={required()} placeholder="vd: interface-name" />
              <TextField name="alias_key" label="Alias Key" required validate={required()} placeholder="vd: if_name" />
              <DropdownField name="lv_kind" label="lv_kind" required options={LV_OPTIONS} validate={required()} />
              {lk === 1 && (
                <>
                  <TextField name="lv_pattern" label="lv_pattern (regex)" required validate={required()} placeholder="^Gi(\\d.*)$" />
                  <TextField name="lv_replace" label="lv_replace" required validate={required()} placeholder="GigabitEthernet$1" />
                </>
              )}
              {lk === 2 && (
                <TextAreaField name="lv_mapping_text" label="lv_mapping (JSON)" rows={4} placeholder='{"idle":"DOWN","established":"UP"}' />
              )}
              <NumberField name="priority" label="Priority" />

              {lk !== 0 && (
                <div className="field-row" style={{ background: "#f0f7ff", padding: 10, borderRadius: 4 }}>
                  <label>Preview transform (value mẫu)</label>
                  <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                    <InputText
                      value={sample}
                      placeholder="nhập value mẫu"
                      onChange={(e) => setSample(e.target.value)}
                      style={{ fontSize: 13 }}
                    />
                    <i className="pi pi-arrow-right" />
                    <strong style={{ color: "#1890ff" }}>{previewTransform(lk, sample, values)}</strong>
                  </div>
                </div>
              )}

              <div style={{ textAlign: "right", marginTop: 8 }}>
                <Button label="Hủy" className="p-button-text" type="button" onClick={onHide} />
                <Button label="Lưu" icon="pi pi-check" type="submit" loading={submitting} />
              </div>
            </form>
          );
        }}
      />
    </Dialog>
  );
};

export default LabelAliasDialog;
