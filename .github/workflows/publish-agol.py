#!/usr/bin/env python3
"""
Publishes ArcGIS Pro add-in to ArcGIS Online using arcgis-python-api.

Uploads the .esriAddInX file and optionally updates metadata (title,
description version line, tags) on the existing AGOL item.
"""

import os
import sys
import argparse
from pathlib import Path

if sys.platform == "win32":
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

try:
    from arcgis.gis import GIS
    from arcgis import __version__ as arcgis_version
except ImportError:
    print("ERROR: arcgis-python-api is not installed. Install with: pip install arcgis")
    sys.exit(1)


def update_version_line(current_description: str, new_version_line: str) -> str:
    """Replace the 'Latest Version:' line in the description, or prepend it."""
    lines = (current_description or "").split('\n')
    updated = []
    replaced = False
    for line in lines:
        if line.strip().startswith('Latest Version:'):
            updated.append(new_version_line)
            replaced = True
        else:
            updated.append(line)
    if not replaced:
        return new_version_line + "\n\n" + (current_description or "")
    return '\n'.join(updated)


def main():
    parser = argparse.ArgumentParser(description="Publish ArcGIS Pro add-in to ArcGIS Online")
    parser.add_argument("--addin-file", required=True, help="Path to the .esriAddInX file")
    parser.add_argument("--item-id", required=True, help="Existing AGOL item ID")
    parser.add_argument("--portal-url", default="https://www.arcgis.com", help="Portal URL")
    parser.add_argument("--auth-method", choices=["token", "username"], default="token")
    parser.add_argument("--client-id", help="OAuth2 client ID (for token auth)")
    parser.add_argument("--client-secret", help="OAuth2 client secret (for token auth)")
    parser.add_argument("--username", help="Portal username (for username auth)")
    parser.add_argument("--password", help="Portal password (for username auth)")
    parser.add_argument("--title", help="Item title (optional)")
    parser.add_argument("--description", help="Description or 'Latest Version: ...' line (optional)")
    parser.add_argument("--tags", help="Comma-separated tags (optional)")

    args = parser.parse_args()

    addin_path = Path(args.addin_file)
    if not addin_path.exists():
        print(f"ERROR: Add-in file not found: {addin_path}")
        sys.exit(1)

    if args.auth_method == "token":
        if not args.client_id or not args.client_secret:
            print("ERROR: --client-id and --client-secret required for token auth")
            sys.exit(1)
    else:
        if not args.username or not args.password:
            print("ERROR: --username and --password required for username auth")
            sys.exit(1)

    print(f"Portal: {args.portal_url}  |  Item: {args.item_id}  |  Auth: {args.auth_method}")
    print(f"Add-in: {addin_path} ({addin_path.stat().st_size / 1024 / 1024:.2f} MB)")
    print(f"arcgis-python-api {arcgis_version}")
    print()

    try:
        # Authenticate
        print("[AUTH] Connecting...")
        if args.auth_method == "token":
            gis = GIS(url=args.portal_url, client_id=args.client_id, client_secret=args.client_secret)
        else:
            gis = GIS(url=args.portal_url, username=args.username, password=args.password)

        user = gis.users.me
        print(f"[OK]   Logged in as: {user.username if user else 'application (OAuth2)'}")

        # Get item
        item = gis.content.get(args.item_id)
        if not item:
            print(f"ERROR: Item {args.item_id} not found or inaccessible")
            sys.exit(1)
        print(f"[OK]   Found item: {item.title}")

        # Build metadata updates
        metadata = {}
        if args.title:
            metadata["title"] = args.title
        if args.description:
            if args.description.startswith("Latest Version:"):
                metadata["description"] = update_version_line(item.description, args.description.strip())
            else:
                metadata["description"] = args.description
        if args.tags:
            metadata["tags"] = [t.strip() for t in args.tags.split(",")]

        # Upload file
        print(f"[UPLOAD] Uploading {addin_path.name}...")
        if not item.update(data=str(addin_path)):
            print("WARNING: upload returned False – file may still have been uploaded")
        else:
            print("[OK]   File uploaded")

        # Update metadata
        if metadata:
            print(f"[META] Updating: {', '.join(metadata.keys())}...")
            if not item.update(item_properties=metadata):
                print("WARNING: metadata update returned False")
            else:
                print("[OK]   Metadata updated")

        # Single verification pass
        verified = gis.content.get(args.item_id)
        print()
        print(f"[DONE] Published successfully")
        print(f"       URL: {args.portal_url}/home/item.html?id={args.item_id}")
        print(f"       Modified: {verified.modified}")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
