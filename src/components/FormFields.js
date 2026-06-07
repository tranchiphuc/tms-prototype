import React from "react";
import { Field } from "react-final-form";
import { InputText } from "primereact/inputtext";
import { InputTextarea } from "primereact/inputtextarea";
import { InputNumber } from "primereact/inputnumber";
import { Dropdown } from "primereact/dropdown";
import { MultiSelect } from "primereact/multiselect";
import { Checkbox } from "primereact/checkbox";

const Wrap = ({ label, hint, error, touched, children, required }) => (
  <div className="field-row">
    {label && (
      <label>
        {label} {required && <span style={{ color: "#ff4d4f" }}>*</span>}
      </label>
    )}
    {children}
    {touched && error && <span className="field-error">{error}</span>}
    {hint && <span className="field-hint">{hint}</span>}
  </div>
);

export const TextField = ({ name, label, hint, required, disabled, placeholder, validate }) => (
  <Field name={name} validate={validate}>
    {({ input, meta }) => (
      <Wrap label={label} hint={hint} required={required} error={meta.error} touched={meta.touched}>
        <InputText
          {...input}
          disabled={disabled}
          placeholder={placeholder}
          style={{ width: "100%", fontSize: 13 }}
        />
      </Wrap>
    )}
  </Field>
);

export const TextAreaField = ({ name, label, hint, required, rows = 3, placeholder, validate }) => (
  <Field name={name} validate={validate}>
    {({ input, meta }) => (
      <Wrap label={label} hint={hint} required={required} error={meta.error} touched={meta.touched}>
        <InputTextarea
          {...input}
          rows={rows}
          placeholder={placeholder}
          autoResize
          style={{ width: "100%", fontSize: 13 }}
        />
      </Wrap>
    )}
  </Field>
);

export const NumberField = ({ name, label, hint, required, mode, minFractionDigits, maxFractionDigits, validate }) => (
  <Field name={name} validate={validate}>
    {({ input, meta }) => (
      <Wrap label={label} hint={hint} required={required} error={meta.error} touched={meta.touched}>
        <InputNumber
          value={input.value === "" || input.value == null ? null : Number(input.value)}
          onValueChange={(e) => input.onChange(e.value)}
          onBlur={input.onBlur}
          mode={mode}
          minFractionDigits={minFractionDigits}
          maxFractionDigits={maxFractionDigits}
          style={{ width: "100%" }}
          inputStyle={{ width: "100%", fontSize: 13 }}
        />
      </Wrap>
    )}
  </Field>
);

export const DropdownField = ({ name, label, hint, required, options, disabled, placeholder, validate }) => (
  <Field name={name} validate={validate}>
    {({ input, meta }) => (
      <Wrap label={label} hint={hint} required={required} error={meta.error} touched={meta.touched}>
        <Dropdown
          value={input.value}
          options={options}
          onChange={(e) => input.onChange(e.value)}
          disabled={disabled}
          placeholder={placeholder}
          style={{ width: "100%", fontSize: 13 }}
        />
      </Wrap>
    )}
  </Field>
);

export const MultiSelectField = ({ name, label, hint, required, options, placeholder, filter, validate }) => (
  <Field name={name} validate={validate}>
    {({ input, meta }) => (
      <Wrap label={label} hint={hint} required={required} error={meta.error} touched={meta.touched}>
        <MultiSelect
          value={input.value || []}
          options={options}
          onChange={(e) => input.onChange(e.value)}
          placeholder={placeholder}
          filter={filter}
          display="chip"
          style={{ width: "100%", fontSize: 13 }}
        />
      </Wrap>
    )}
  </Field>
);

export const CheckboxField = ({ name, label, hint }) => (
  <Field name={name} type="checkbox">
    {({ input }) => (
      <div className="field-row" style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <Checkbox inputId={name} checked={!!input.checked} onChange={(e) => input.onChange(e.checked)} />
        <label htmlFor={name} style={{ margin: 0 }}>
          {label}
        </label>
        {hint && <span className="field-hint">{hint}</span>}
      </div>
    )}
  </Field>
);

export const required = (msg = "Bắt buộc") => (v) =>
  v === undefined || v === null || v === "" ? msg : undefined;
