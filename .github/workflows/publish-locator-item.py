#!/usr/bin/env python3
"""
Publish or update a locator package (.gcpk) item in ArcGIS Online/Portal.
"""

import argparse
import os
import sys
from pathlib import Path

if sys.platform == "win32":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    os.environ["PYTHONIOENCODING"] = "utf-8"

try:
    from arcgis.gis import GIS
except ImportError:
    print("ERROR: arcgis-python-api is not installed. Install with: pip install arcgis")
    sys.exit(1)


def connect(args):
    if args.auth_method == "token":
        if not args.client_id or not args.client_secret:
            raise ValueError("--client-id and --client-secret are required for token auth")
        return GIS(url=args.portal_url, client_id=args.client_id, client_secret=args.client_secret)
    if not args.username or not args.password:
        raise ValueError("--username and --password are required for username auth")
    return GIS(url=args.portal_url, username=args.username, password=args.password)


def main():
    parser = argparse.ArgumentParser(description="Publish locator package to ArcGIS Online/Portal")
    parser.add_argument("--locator-package", required=True, help="Path to .gcpk file")
    parser.add_argument("--portal-url", default="https://www.arcgis.com")
    parser.add_argument("--auth-method", choices=["token", "username"], default="token")
    parser.add_argument("--client-id")
    parser.add_argument("--client-secret")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--item-id", help="Existing item id to update (optional)")
    parser.add_argument("--title", default="Overture Hybrid Locator")
    parser.add_argument("--tags", default="ArcGIS,Locator,Geocoding,Overture")
    parser.add_argument("--folder", help="Portal folder name for item create (optional)")
    args = parser.parse_args()

    locator_path = Path(args.locator_package)
    if not locator_path.exists():
        print(f"ERROR: Locator package not found: {locator_path}")
        sys.exit(1)

    if locator_path.suffix.lower() != ".gcpk":
        print(f"ERROR: Expected a .gcpk file, got: {locator_path.name}")
        sys.exit(1)

    try:
        gis = connect(args)
        user = gis.users.me
        print(f"[OK] Authenticated as: {user.username if user else 'application'}")

        tags = [t.strip() for t in (args.tags or "").split(",") if t.strip()]
        if args.item_id:
            item = gis.content.get(args.item_id)
            if not item:
                print(f"ERROR: Could not find item {args.item_id}")
                sys.exit(1)
            print(f"[INFO] Updating locator item: {item.title}")
            ok = item.update(
                item_properties={"title": args.title, "tags": tags},
                data=str(locator_path),
            )
            if not ok:
                print("ERROR: Locator item update returned False")
                sys.exit(1)
            print(f"[DONE] Updated locator item: {args.portal_url}/home/item.html?id={item.id}")
            return

        print("[INFO] Creating new locator package item")
        item_props = {
            "title": args.title,
            "type": "Locator Package",
            "tags": tags,
            "snippet": "Hybrid Overture address + place locator package",
        }
        item = gis.content.add(item_properties=item_props, data=str(locator_path), folder=args.folder)
        if not item:
            print("ERROR: Failed to create locator package item")
            sys.exit(1)
        print(f"[DONE] Created locator item: {args.portal_url}/home/item.html?id={item.id}")
    except Exception as ex:
        print(f"ERROR: {ex}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
