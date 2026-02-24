import geopandas as gpd
import pandas as pd
import sqlite3


def duplicate_pointlayer_for_duplicate_tablelayer(
    src_gpkg_path,
    dst_gpkg_path="hydamo_duplicates.gpkg",
    pointlayer="gemaal",
    tablelayer="pomp",
    layerid="gemaalid",
):
    """
    Duplicate point features in 'pointlayer' for each duplicate reference in 'tablelayer'.
    For each set of rows in 'tablelayer' that reference the same 'globalid' in 'pointlayer', create duplicate point features with new 'globalid's and update the 'tablelayer' references accordingly.
    Parameters:
    - src_gpkg_path: Path to the source GeoPackage containing the original layers.
    - dst_gpkg_path: Path to the output GeoPackage to create with duplicates.
    - pointlayer: Name of the point layer in the source GeoPackage (default 'gemaal
    ').
    - tablelayer: Name of the non-spatial table layer in the source GeoPackage that references
        the point layer (default 'pomp').
    - layerid: Name of the column in 'tablelayer' that references 'pointlayer.globalid' (default 'gemaalid').
    Returns:
    - A dictionary with output information, including the path to the new GeoPackage and the number of duplicates created.
    """

    # Read source layers
    point_gdf = gpd.read_file(src_gpkg_path, layer=pointlayer)
    table_df = gpd.read_file(src_gpkg_path, layer=tablelayer)  # NO geometry

    if "globalid" not in point_gdf.columns:
        raise ValueError("point layer must contain 'globalid' column.")
    if layerid not in table_df.columns:
        raise ValueError(
            f"table layer must contain '{layerid}' column referencing point.globalid."
        )

    # Work on a copy
    updated_table = table_df.copy()

    # Group table rows by layerid
    table_groups = updated_table.groupby(layerid).apply(lambda df: df.index.tolist())
    duplicates_to_process = {k: v for k, v in table_groups.items() if len(v) > 1}

    new_point_layer_rows = []

    for original_point_layer_id, table_indices in duplicates_to_process.items():
        orig = point_gdf[point_gdf["globalid"] == original_point_layer_id]
        if orig.empty:
            continue
        orig_row = orig.iloc[0]

        # First table keeps original layerid; others get copies
        for i, table_idx in enumerate(table_indices[1:], start=1):
            new_row = orig_row.copy()
            new_globalid = f"{original_point_layer_id}_{i}"
            new_row["globalid"] = new_globalid
            new_point_layer_rows.append(new_row)

            updated_table.at[table_idx, layerid] = new_globalid

    # Build output point (GeoDataFrame)
    if new_point_layer_rows:
        new_point_df = pd.DataFrame(new_point_layer_rows)
        geom_col = point_gdf.geometry.name
        new_point_gdf = gpd.GeoDataFrame(
            new_point_df, geometry=geom_col, crs=point_gdf.crs
        )
        point_out = pd.concat([point_gdf, new_point_gdf], ignore_index=True)
    else:
        point_out = point_gdf

    # 1) Write spatial point layer with GeoPandas
    point_out.to_file(dst_gpkg_path, layer=pointlayer, driver="GPKG")

    # 2) Write non-spatial table as SQLite table and register it in gpkg system tables
    conn = sqlite3.connect(dst_gpkg_path)
    try:
        # Write DataFrame as a regular table; overwrite if exists
        updated_table.to_sql(tablelayer, conn, if_exists="replace", index=False)

        # Register in gpkg_contents as an 'attributes' (aspatial) table
        cur = conn.cursor()
        # Create gpkg_contents row if needed
        cur.execute(
            """
            INSERT OR REPLACE INTO gpkg_contents
            (table_name, data_type, identifier, description, last_change)
            VALUES (?, 'attributes', ?, '', datetime('now'))
            """,
            (tablelayer, tablelayer),
        )
        conn.commit()
    finally:
        conn.close()

    return {
        "output_gpkg": dst_gpkg_path,
        "duplicates_created": len(new_point_layer_rows),
    }


if __name__ == "__main__":
    hydamo_path = r"path\to\your\input\HyDAMO.gpkg"
    output_path = r"path\to\your\output\HyDAMO_duplicates.gpkg"
    output = duplicate_pointlayer_for_duplicate_tablelayer(
        hydamo_path,
        dst_gpkg_path=output_path,
        pointlayer="stuw",
        tablelayer="kunstwerkopening",
        layerid="stuwid",
    )

    print(f"Output GeoPackage: {output['output_gpkg']}")
    print(f"Number of duplicates created: {output['duplicates_created']}")
