#!/usr/bin/env python3
"""
Publish a locator as a geocode service to stand-alone ArcGIS Server.

This script is intended for ArcGIS Pro Python environments where arcpy is available.
"""

import argparse
import os
import sys
from pathlib import Path

try:
    import arcpy
except ImportError:
    print("ERROR: arcpy is required. Run this script inside an ArcGIS Pro Python environment.")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Publish locator to ArcGIS Server geocode service")
    parser.add_argument("--locator-path", required=True, help="Path to locator (.loc)")
    parser.add_argument("--server-connection", required=True, help="Path to ArcGIS Server connection file (.ags)")
    parser.add_argument("--service-name", required=True, help="Service name")
    parser.add_argument("--out-folder", default="release", help="Output folder for draft/sd files")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting existing service")
    args = parser.parse_args()

    locator_path = Path(args.locator_path)
    server_conn = Path(args.server_connection)
    out_folder = Path(args.out_folder)
    out_folder.mkdir(parents=True, exist_ok=True)

    if not locator_path.exists():
        print(f"ERROR: locator path not found: {locator_path}")
        sys.exit(1)
    if not server_conn.exists():
        print(f"ERROR: server connection not found: {server_conn}")
        sys.exit(1)

    sddraft_path = out_folder / f"{args.service_name}.sddraft"
    sd_path = out_folder / f"{args.service_name}.sd"

    try:
        arcpy.env.overwriteOutput = bool(args.overwrite)

        # Create geocode service definition draft from locator
        arcpy.CreateGeocodeSDDraft(
            in_locator=str(locator_path),
            out_sddraft=str(sddraft_path),
            service_name=args.service_name,
            connection_file_path=str(server_conn),
            copy_data_to_server=True,
            folder_name=None,
            summary="Overture hybrid locator geocode service",
            tags="ArcGIS,Locator,Geocode,Overture",
        )
        print(f"[OK] Created SDDraft: {sddraft_path}")

        # Stage and upload
        arcpy.server.StageService(in_service_definition_draft=str(sddraft_path), out_service_definition=str(sd_path))
        print(f"[OK] Staged service definition: {sd_path}")

        arcpy.server.UploadServiceDefinition(
            in_sd_file=str(sd_path),
            in_server=str(server_conn),
            in_service_name=args.service_name,
        )
        print(f"[DONE] Published geocode service: {args.service_name}")
    except Exception as ex:
        print(f"ERROR: {ex}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
