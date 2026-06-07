import React, { useEffect, useState, useRef, useCallback } from "react";
import { useDispatch, useSelector } from "react-redux";
import { DataTable } from "primereact/datatable";
import { Column } from "primereact/column";
import { Button } from "primereact/button";
import { fetchList } from "../redux/actions/flinkActions";
import FilterBar from "./FilterBar";
import StatusBadge from "./StatusBadge";

// Shell dùng chung cho 4 tab Flink Rule.
// Props:
//   slice, title, searchPlaceholder
//   columns: [{ field, header, sortable, body, style, isLink, onLinkClick }]
//   onAdd, onEdit, onDelete, onPreview (tùy chọn)
//   reloadToken (number): đổi giá trị → refetch
//   toolbarExtra: node
const RuleListShell = ({
  slice,
  title,
  searchPlaceholder,
  columns,
  onAdd,
  onEdit,
  onDelete,
  onPreview,
  reloadToken,
  toolbarExtra,
}) => {
  const dispatch = useDispatch();
  const sliceState = useSelector((s) => s.flink[slice]) || { list: [], loading: false, total: 0 };
  const [filters, setFilters] = useState({ q: "", vendor_code: "", status: 1 });
  const filtersRef = useRef(filters);
  filtersRef.current = filters;

  const load = useCallback(() => {
    const f = filtersRef.current;
    const params = { page_size: 200, sort: "-updated_at" };
    if (f.q) params.q = f.q;
    if (f.vendor_code) params.vendor_code = f.vendor_code;
    params.status = f.status; // "" = tất cả
    dispatch(fetchList(slice, params));
  }, [dispatch, slice]);

  // Mount + khi reloadToken đổi
  useEffect(() => {
    load();
  }, [load, reloadToken]);

  const list = sliceState.list;

  const sttBody = (row) => list.indexOf(row) + 1;

  const actionBody = (row) => (
    <div className="row-actions">
      {onPreview && (
        <Button
          icon="pi pi-eye"
          className="p-button-text p-button-sm"
          tooltip="Preview"
          tooltipOptions={{ position: "top" }}
          style={{ color: "#1890ff" }}
          onClick={() => onPreview(row)}
        />
      )}
      <Button
        icon="pi pi-pencil"
        className="action-icon-btn edit p-button-text p-button-sm"
        tooltip="Sửa"
        tooltipOptions={{ position: "top" }}
        onClick={() => onEdit(row)}
      />
      <Button
        icon="pi pi-trash"
        className="action-icon-btn delete p-button-text p-button-sm"
        tooltip="Deprecate"
        tooltipOptions={{ position: "top" }}
        onClick={() => onDelete(row)}
      />
    </div>
  );

  return (
    <div>
      <div className="page-header">
        <FilterBar
          value={filters}
          onChange={setFilters}
          onSearch={load}
          searchPlaceholder={searchPlaceholder}
        />
        <div className="page-actions">
          {toolbarExtra}
          <Button label="Thêm mới" icon="pi pi-plus" className="btn-add" onClick={onAdd} />
        </div>
      </div>

      <DataTable
        value={list}
        loading={sliceState.loading}
        paginator
        rows={20}
        rowsPerPageOptions={[20, 50, 100]}
        dataKey="id"
        responsiveLayout="scroll"
        className="rule-table"
        emptyMessage="Không có dữ liệu"
        currentPageReportTemplate="Hiển thị {first} đến {last} trên tổng số {totalRecords} bản ghi"
        paginatorTemplate="CurrentPageReport FirstPageLink PrevPageLink PageLinks NextPageLink LastPageLink RowsPerPageDropdown"
      >
        <Column header="STT" body={sttBody} style={{ width: 60 }} />
        {columns.map((c) => (
          <Column
            key={c.field}
            field={c.field}
            header={c.header}
            sortable={c.sortable !== false}
            style={c.style}
            body={
              c.body
                ? c.body
                : c.isLink
                ? (row) => (
                    <span className="cell-link" onClick={() => (c.onLinkClick || onEdit)(row)}>
                      {row[c.field]}
                    </span>
                  )
                : undefined
            }
          />
        ))}
        <Column
          header="Trạng thái"
          field="status"
          sortable
          style={{ width: 130 }}
          body={(row) => <StatusBadge status={row.status} />}
        />
        <Column header="Hành động" body={actionBody} style={{ width: 132 }} />
      </DataTable>
    </div>
  );
};

export default RuleListShell;
