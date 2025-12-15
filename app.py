from functools import lru_cache
import math
from datetime import datetime

from databricks import sql
from databricks.sdk.core import Config
import pandas as pd

import dash
from dash import dash_table, html, dcc, no_update
from dash.dependencies import Input, Output, State

cfg = Config()

@lru_cache(maxsize=1)
def get_connection(http_path):
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

def read_table(table_name: str, conn) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        return cursor.fetchall_arrow().to_pandas()

def sql_literal(v):
    import math as _math
    if v is None or (isinstance(v, float) and _math.isnan(v)):
        return "NULL"
    if isinstance(v, str):
        # escape single quotes for SQL
        return "'" + v.replace("'", "''") + "'"
    return str(v)

def insert_overwrite_table(table_name: str, df: pd.DataFrame, conn):
    with conn.cursor() as cursor:
        rows = list(df.itertuples(index=False))
        values_list = []
        for row in rows:
            formatted = ",".join(sql_literal(v) for v in row)
            values_list.append(f"({formatted})")
        values = ",".join(values_list)
        sql_stmt = f"INSERT OVERWRITE {table_name} VALUES {values}"
        cursor.execute(sql_stmt)


# --------------------------------------------------
# Databricks config (warehouse is fixed; table is dynamic)
# --------------------------------------------------
http_path_input = "/sql/1.0/warehouses/148ccb90800933a1"
conn = get_connection(http_path_input)

PAGE_SIZE = 20

# --------------------------------------------------
# Dash app
# --------------------------------------------------
app = dash.Dash(__name__)

PRIMARY = "#2563eb"
BORDER = "#e5e7eb"
BG_LIGHT = "#f3f4f6"

app.layout = html.Div(
    style={
        "minHeight": "100vh",
        "backgroundColor": "#f4f5fb",
        "display": "flex",
        "justifyContent": "center",
        "alignItems": "flex-start",
        "padding": "40px",
        "fontFamily": (
            "system-ui, -apple-system, BlinkMacSystemFont, "
            "'Segoe UI', sans-serif"
        ),
    },
    children=[
        # hidden stores
        dcc.Store(id="table-name-store"),        # full table name: catalog.schema.data_dict
        dcc.Store(id="full-data-store"),         # full data_dict as records
        dcc.Store(id="view-mode-store"),         # "tables" or "columns"
        dcc.Store(id="selected-table-store"),    # current table name in columns view

        html.Div(
            style={
                "width": "100%",
                "maxWidth": "1200px",
                "backgroundColor": "white",
                "borderRadius": "16px",
                "boxShadow": "0 10px 30px rgba(15, 23, 42, 0.12)",
                "padding": "24px 28px 32px",
                "border": f"1px solid {BORDER}",
            },
            children=[
                # Header
                html.Div(
                    style={
                        "display": "flex",
                        "justifyContent": "space-between",
                        "alignItems": "center",
                        "marginBottom": "12px",
                    },
                    children=[
                        html.Div(
                            children=[
                                html.H2(
                                    "Data Dictionary Editor",
                                    style={
                                        "margin": 0,
                                        "fontSize": "22px",
                                        "fontWeight": 600,
                                        "color": "#111827",
                                    },
                                ),
                                html.Div(
                                    id="table-label",
                                    children="Table: (none loaded)",
                                    style={
                                        "marginTop": "4px",
                                        "fontSize": "13px",
                                        "color": "#6b7280",
                                    },
                                ),
                            ]
                        ),
                        html.Div(
                            "Editable · Paginated · Backed by Delta",
                            style={
                                "fontSize": "12px",
                                "color": "#9ca3af",
                                "textTransform": "uppercase",
                                "letterSpacing": "0.08em",
                                "fontWeight": 600,
                            },
                        ),
                    ],
                ),

                html.Hr(style={"borderColor": BORDER, "margin": "12px 0 18px"}),

                # Catalog / schema selector
                html.Div(
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "gap": "10px",
                        "marginBottom": "12px",
                        "flexWrap": "wrap",
                    },
                    children=[
                        dcc.Input(
                            id="catalog-input",
                            type="text",
                            placeholder="Catalog",
                            style={
                                "padding": "6px 10px",
                                "borderRadius": "999px",
                                "border": f"1px solid {BORDER}",
                                "fontSize": "12px",
                            },
                        ),
                        dcc.Input(
                            id="schema-input",
                            type="text",
                            placeholder="Schema",
                            style={
                                "padding": "6px 10px",
                                "borderRadius": "999px",
                                "border": f"1px solid {BORDER}",
                                "fontSize": "12px",
                            },
                        ),
                        html.Button(
                            "Load dictionary",
                            id="load-btn",
                            n_clicks=0,
                            style={
                                "backgroundColor": "white",
                                "color": "#374151",
                                "border": f"1px solid {BORDER}",
                                "borderRadius": "999px",
                                "padding": "7px 14px",
                                "fontSize": "12px",
                                "cursor": "pointer",
                            },
                        ),
                        html.Div(
                            id="load-status",
                            style={
                                "fontSize": "12px",
                                "color": "#6b7280",
                                "fontStyle": "italic",
                                "minHeight": "18px",
                            },
                        ),
                    ],
                ),

                # Controls row (Back + Save + pager)
                html.Div(
                    style={
                        "display": "flex",
                        "justifyContent": "space-between",
                        "alignItems": "center",
                        "marginBottom": "12px",
                        "gap": "12px",
                        "flexWrap": "wrap",
                    },
                    children=[
                        html.Div(
                            style={
                                "display": "flex",
                                "alignItems": "center",
                                "gap": "10px",
                                "flexWrap": "wrap",
                            },
                            children=[
                                html.Button(
                                    "← Back to tables",
                                    id="back-to-tables-btn",
                                    n_clicks=0,
                                    style={
                                        "backgroundColor": "white",
                                        "color": "#374151",
                                        "border": f"1px solid {BORDER}",
                                        "borderRadius": "999px",
                                        "padding": "6px 12px",
                                        "fontSize": "12px",
                                        "cursor": "pointer",
                                    },
                                ),
                                html.Button(
                                    "Save changes",
                                    id="save-btn",
                                    n_clicks=0,
                                    style={
                                        "backgroundColor": PRIMARY,
                                        "color": "white",
                                        "border": "none",
                                        "borderRadius": "999px",
                                        "padding": "8px 16px",
                                        "fontSize": "13px",
                                        "fontWeight": 500,
                                        "cursor": "pointer",
                                        "boxShadow": "0 4px 10px rgba(37, 99, 235, 0.35)",
                                    },
                                ),
                                html.Div(
                                    id="save-status",
                                    style={
                                        "fontSize": "12px",
                                        "color": "#6b7280",
                                        "fontStyle": "italic",
                                        "minHeight": "18px",
                                    },
                                ),
                            ],
                        ),
                        html.Div(
                            style={
                                "display": "flex",
                                "alignItems": "center",
                                "gap": "8px",
                                "backgroundColor": BG_LIGHT,
                                "borderRadius": "999px",
                                "padding": "4px 8px",
                                "border": f"1px solid {BORDER}",
                            },
                            children=[
                                html.Button(
                                    "← Previous",
                                    id="prev-page",
                                    n_clicks=0,
                                    style={
                                        "backgroundColor": "transparent",
                                        "border": "none",
                                        "padding": "4px 10px",
                                        "fontSize": "12px",
                                        "cursor": "pointer",
                                        "borderRadius": "999px",
                                    },
                                ),
                                html.Span(
                                    id="page-indicator",
                                    style={
                                        "fontSize": "12px",
                                        "color": "#4b5563",
                                        "minWidth": "110px",
                                        "textAlign": "center",
                                    },
                                ),
                                html.Button(
                                    "Next →",
                                    id="next-page",
                                    n_clicks=0,
                                    style={
                                        "backgroundColor": "transparent",
                                        "border": "none",
                                        "padding": "4px 10px",
                                        "fontSize": "12px",
                                        "cursor": "pointer",
                                        "borderRadius": "999px",
                                    },
                                ),
                            ],
                        ),
                    ],
                ),

                # Data table
                dash_table.DataTable(
                    id="db-table",
                    data=[],          # starts empty until a table is loaded
                    columns=[],
                    editable=True,
                    page_size=PAGE_SIZE,
                    page_current=0,
                    page_action="native",
                    style_as_list_view=True,
                    style_table={
                        "overflowX": "auto",
                        "maxHeight": "65vh",
                        "border": f"1px solid {BORDER}",
                        "borderRadius": "12px",
                    },
                    style_header={
                        "backgroundColor": "#f9fafb",
                        "fontWeight": 600,
                        "fontSize": "12px",
                        "borderBottom": f"1px solid {BORDER}",
                        "color": "#4b5563",
                    },
                    style_cell={
                        "whiteSpace": "normal",
                        "height": "auto",
                        "textAlign": "left",
                        "padding": "6px 10px",
                        "fontSize": "12px",
                        "borderBottom": f"1px solid {BORDER}",
                    },
                    style_data={
                        "backgroundColor": "white",
                    },
                    style_data_conditional=[
                        {
                            "if": {"row_index": "odd"},
                            "backgroundColor": "#f9fafb",
                        },
                    ],
                ),

                html.Div(
                    "Changes are applied to the underlying Delta table when you click Save.",
                    style={
                        "marginTop": "10px",
                        "fontSize": "11px",
                        "color": "#9ca3af",
                    },
                ),
            ],
        )
    ],
)

# --------------------------------------------------
# Utility functions for view switching
# --------------------------------------------------

def make_tables_view(df: pd.DataFrame):
    """
    TABLES view:
      - only description is editable
      - adds an __open__ column for drill-down
    """
    if "type" in df.columns:
        tables_df = df[df["type"] == "table"].copy()
    else:
        tables_df = df.copy()

    if tables_df.empty:
        tables_df["__open__"] = []
    else:
        tables_df["__open__"] = "Open"

    columns = []

    if "name" in tables_df.columns:
        columns.append({
            "name": "Table",
            "id": "name",
            "editable": False,    # not editable
            "type": "text",
            "presentation": "input",
        })

    if "description" in tables_df.columns:
        columns.append({
            "name": "Description",
            "id": "description",
            "editable": True,     # ONLY editable column
            "type": "text",
            "presentation": "input",
        })

    if "parent" in tables_df.columns:
        columns.append({
            "name": "Schema",
            "id": "parent",
            "editable": False,    # not editable
            "type": "text",
            "presentation": "input",
        })

    columns.append({
        "name": "",
        "id": "__open__",
        "editable": False,        # action column not editable
        "type": "text",
        "presentation": "markdown",
    })

    return tables_df.to_dict("records"), columns


def make_columns_view(df: pd.DataFrame, table_name: str):
    """
    COLUMNS view:
      - only description is editable
      - shows all columns for the selected table
    """
    if "type" in df.columns:
        cols_df = df[(df["parent"] == table_name) & (df["type"] != "table")].copy()
    else:
        cols_df = df[df["parent"] == table_name].copy()

    columns = []

    if "name" in cols_df.columns:
        columns.append({
            "name": "Column",
            "id": "name",
            "editable": False,    # not editable
            "type": "text",
            "presentation": "input",
        })

    if "description" in cols_df.columns:
        columns.append({
            "name": "Description",
            "id": "description",
            "editable": True,     # ONLY editable column
            "type": "text",
            "presentation": "input",
        })

    if "parent" in cols_df.columns:
        columns.append({
            "name": "Table",
            "id": "parent",
            "editable": False,    # not editable
            "type": "text",
            "presentation": "input",
        })

    if "type" in cols_df.columns:
        columns.append({
            "name": "Type",
            "id": "type",
            "editable": False,    # not editable
            "type": "text",
            "presentation": "input",
        })

    return cols_df.to_dict("records"), columns


# --------------------------------------------------
# MASTER callback: load, drill-down, back, save
# --------------------------------------------------
@app.callback(
    Output("db-table", "data"),
    Output("db-table", "columns"),
    Output("table-label", "children"),
    Output("load-status", "children"),
    Output("table-name-store", "data"),
    Output("full-data-store", "data"),
    Output("view-mode-store", "data"),
    Output("selected-table-store", "data"),
    Output("save-status", "children"),
    Input("load-btn", "n_clicks"),
    Input("db-table", "active_cell"),
    Input("back-to-tables-btn", "n_clicks"),
    Input("save-btn", "n_clicks"),
    State("catalog-input", "value"),
    State("schema-input", "value"),
    State("view-mode-store", "data"),
    State("selected-table-store", "data"),
    State("table-name-store", "data"),
    State("full-data-store", "data"),
    State("db-table", "data"),
    prevent_initial_call=True,
)
def main_controller(
    load_clicks,
    active_cell,
    back_clicks,
    save_clicks,
    catalog,
    schema,
    view_mode,
    selected_table,
    table_name,
    full_data_raw,
    current_view_rows,
):
    ctx = dash.callback_context
    if not ctx.triggered:
        return (no_update,) * 9

    trigger = ctx.triggered[0]["prop_id"].split(".")[0]

    # Defaults: don't change anything unless we explicitly set it
    data_out = no_update
    columns_out = no_update
    label_out = no_update
    load_status_out = no_update
    table_name_out = no_update
    full_data_out = no_update
    view_mode_out = no_update
    selected_table_out = no_update
    save_status_out = no_update

    # -------------------------
    # 1) LOAD DATA DICTIONARY
    # -------------------------
    if trigger == "load-btn":
        if not catalog or not schema:
            data_out = []
            columns_out = []
            label_out = "Table: (none loaded)"
            load_status_out = "Please enter both catalog and schema."
            table_name_out = None
            full_data_out = None
            view_mode_out = "tables"
            selected_table_out = None
            # no change to save_status
            return (
                data_out,
                columns_out,
                label_out,
                load_status_out,
                table_name_out,
                full_data_out,
                view_mode_out,
                selected_table_out,
                save_status_out,
            )

        catalog = catalog.strip()
        schema = schema.strip()
        new_table_name = f"{catalog}.{schema}.data_dict"

        try:
            df = read_table(new_table_name, conn)
        except Exception as e:
            data_out = []
            columns_out = []
            label_out = f"Table: {new_table_name}"
            load_status_out = f"Error loading {new_table_name}: {e}"
            table_name_out = None
            full_data_out = None
            view_mode_out = "tables"
            selected_table_out = None
            return (
                data_out,
                columns_out,
                label_out,
                load_status_out,
                table_name_out,
                full_data_out,
                view_mode_out,
                selected_table_out,
                save_status_out,
            )

        full_records = df.to_dict("records")
        table_records, columns = make_tables_view(df)

        load_msg = f"Loaded {new_table_name} ({len(df)} rows) at {datetime.now().strftime('%H:%M:%S')}."
        label = f"Tables in data dictionary: {catalog}.{schema}"

        data_out = table_records
        columns_out = columns
        label_out = label
        load_status_out = load_msg
        table_name_out = new_table_name
        full_data_out = full_records
        view_mode_out = "tables"
        selected_table_out = None
        # save_status_out remains no_update

    # -------------------------
    # 2) DRILL DOWN: OPEN TABLE
    # -------------------------
    elif trigger == "db-table":
        # Only handle click on "__open__" when in tables view
        if (
            view_mode == "tables"
            and active_cell is not None
            and active_cell.get("column_id") == "__open__"
            and full_data_raw is not None
        ):
            row_index = active_cell.get("row")
            if row_index is not None and current_view_rows and row_index < len(current_view_rows):
                row = current_view_rows[row_index]
                table_name_in_dict = row.get("name")
            else:
                table_name_in_dict = None

            if table_name_in_dict:
                full_df = pd.DataFrame(full_data_raw)
                col_records, columns = make_columns_view(full_df, table_name_in_dict)
                data_out = col_records
                columns_out = columns
                label_out = f"Columns for table: {table_name_in_dict}"
                view_mode_out = "columns"
                selected_table_out = table_name_in_dict
                # load_status_out, table_name_out, full_data_out, save_status_out unchanged

    # -------------------------
    # 3) BACK TO TABLES VIEW
    # -------------------------
    elif trigger == "back-to-tables-btn":
        if view_mode == "columns" and full_data_raw is not None and table_name:
            full_df = pd.DataFrame(full_data_raw)
            table_records, columns = make_tables_view(full_df)

            # show catalog.schema (strip ".data_dict")
            base = table_name.rsplit(".", 1)[0]
            label = f"Tables in data dictionary: {base}"

            data_out = table_records
            columns_out = columns
            label_out = label
            view_mode_out = "tables"
            selected_table_out = None
            # others unchanged

    # -------------------------
    # 4) SAVE CHANGES
    # -------------------------
    elif trigger == "save-btn":
        if not table_name:
            save_status_out = "No table loaded. Please load a table before saving."
        elif full_data_raw is None:
            save_status_out = "No in-memory copy of the data dictionary. Try reloading it."
        else:
            try:
                full_df = pd.DataFrame(full_data_raw)
                view_df = pd.DataFrame(current_view_rows)

                # remove helper columns not in full_df (e.g. __open__)
                helper_cols = [c for c in view_df.columns if c not in full_df.columns]
                if helper_cols:
                    view_df = view_df.drop(columns=helper_cols)

                if view_mode == "tables":
                    # Update only rows where type == 'table'
                    for _, row in view_df.iterrows():
                        name = row.get("name")
                        parent = row.get("parent")
                        if name is None or parent is None:
                            continue

                        if "type" in full_df.columns:
                            mask = (
                                (full_df["type"] == "table")
                                & (full_df["name"] == name)
                                & (full_df["parent"] == parent)
                            )
                        else:
                            mask = (
                                (full_df["name"] == name)
                                & (full_df["parent"] == parent)
                            )

                        for col in view_df.columns:
                            full_df.loc[mask, col] = row[col]

                elif view_mode == "columns" and selected_table:
                    # Update only rows that belong to the selected table
                    for _, row in view_df.iterrows():
                        name = row.get("name")
                        parent = row.get("parent")
                        typ = row.get("type") if "type" in view_df.columns else None
                        if name is None or parent is None:
                            continue

                        if "type" in full_df.columns and typ is not None:
                            mask = (
                                (full_df["name"] == name)
                                & (full_df["parent"] == parent)
                                & (full_df["type"] == typ)
                            )
                        else:
                            mask = (
                                (full_df["name"] == name)
                                & (full_df["parent"] == parent)
                            )

                        for col in view_df.columns:
                            full_df.loc[mask, col] = row[col]
                else:
                    # Fallback: treat as full edit
                    full_df = view_df

                # write back to Delta
                insert_overwrite_table(table_name, full_df, conn)

                # update in-memory full_data_store
                new_full_records = full_df.to_dict("records")
                full_data_out = new_full_records

                # rebuild current view from updated full_df
                if view_mode == "tables":
                    table_records, columns = make_tables_view(full_df)
                    data_out = table_records
                    columns_out = columns
                    base = table_name.rsplit(".", 1)[0]
                    label_out = f"Tables in data dictionary: {base}"
                elif view_mode == "columns" and selected_table:
                    col_records, columns = make_columns_view(full_df, selected_table)
                    data_out = col_records
                    columns_out = columns
                    label_out = f"Columns for table: {selected_table}"

                save_status_out = f"Saved changes to {table_name} (click #{save_clicks})."
            except Exception as e:
                save_status_out = f"Error saving changes: {e}"

    return (
        data_out,
        columns_out,
        label_out,
        load_status_out,
        table_name_out,
        full_data_out,
        view_mode_out,
        selected_table_out,
        save_status_out,
    )


# --------------------------------------------------
# Pagination buttons (also updates indicator when data changes)
# --------------------------------------------------
@app.callback(
    Output("db-table", "page_current"),
    Output("page-indicator", "children"),
    Input("prev-page", "n_clicks"),
    Input("next-page", "n_clicks"),
    Input("db-table", "data"),
    State("db-table", "page_current"),
    State("db-table", "page_size"),
    prevent_initial_call=True,
)
def change_page(prev_clicks, next_clicks, rows, current_page, page_size):
    ctx = dash.callback_context
    total_rows = len(rows) if rows else 0
    total_pages = max(1, math.ceil(total_rows / page_size)) if page_size else 1

    if not ctx.triggered:
        page_label = f"Page {current_page + 1} of {total_pages}"
        return current_page, page_label

    button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if button_id == "db-table":
        # data changed (new table loaded) → reset to first page
        new_page = 0
    elif button_id == "next-page":
        new_page = min(current_page + 1, total_pages - 1)
    elif button_id == "prev-page":
        new_page = max(current_page - 1, 0)
    else:
        new_page = current_page

    page_label = f"Page {new_page + 1} of {total_pages}"
    return new_page, page_label


if __name__ == "__main__":
    app.run_server(debug=True)
