using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class Catalog
    {
        public string CatalogCode { get; set; }
        public string CatalogDesc { get; set; }
        public int NoScanRepl { get; set; }
        public string GLAccount { get; set; }
        public string CatGroup { get; set; }
    }
}


//SELECT
//    auth_check,
//    auto_invoice,
//    brand,
//    bus_area,
//    cat_class,
//    cat_group,
//    category,
//    catlog_code,
//    catlog_type,
//    description,
//    del_flag,
//    expense_element,
//    gl_account,
//    gl_disburse_code,
//    gl_narration,
//    gl_ref_code,
//    gl_ref_tbl,
//    gst_rate,
//    gst_status,
//    lead_time,
//    man_code,
//    market,
//    pallet_type,
//    pallet_weight,
//    part_number,
//    picture_id,
//    short_desc,
//    status,
//    supplier_code,
//    supply_reference,
//    unit_cost,
//    unit_price,
//    uom_issue,
//    uom_order,
//    uom_pallet,
//    uom_receive,
//    uom_stock,
//    uom_stock_rm,
//    version,
//    warehouse_id,
//    serial_status,
//    browse_available,
//    lot_no_status,
//    gtin_no,
//    def_pallet_qty,
//    def_order_qty,
//    no_scan_repl,
//    is_run_out,
//    shelf_life,
//    min_shelf_life,
//    max_stock_holding
//    FROM pu_catalog
//    WHERE catlog_code = ?