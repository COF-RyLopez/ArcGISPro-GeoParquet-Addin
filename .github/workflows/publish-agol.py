#!/usr/bin/env python3
"""
Publishes ArcGIS Pro add-in to ArcGIS Online using arcgis-python-api

This script uploads and updates an add-in item in ArcGIS Online.
Supports both OAuth2 app client token and username/password authentication.
"""

import sys
import os
import argparse
from pathlib import Path

try:
    from arcgis.gis import GIS
    from arcgis import __version__ as arcgis_version
except ImportError:
    print("ERROR: arcgis-python-api is not installed.")
    print("Please install it with: pip install arcgis")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Publish ArcGIS Pro add-in to ArcGIS Online"
    )
    parser.add_argument(
        "--addin-file",
        required=True,
        help="Path to the .esriAddInX file to publish"
    )
    parser.add_argument(
        "--item-id",
        required=True,
        help="The item ID of the existing add-in in AGOL"
    )
    parser.add_argument(
        "--portal-url",
        default="https://www.arcgis.com",
        help="ArcGIS Online portal URL (default: https://www.arcgis.com)"
    )
    
    # Authentication options
    auth_group = parser.add_mutually_exclusive_group(required=True)
    auth_group.add_argument(
        "--client-id",
        help="OAuth2 app client ID"
    )
    auth_group.add_argument(
        "--username",
        help="Portal username"
    )
    
    parser.add_argument(
        "--client-secret",
        help="OAuth2 app client secret (required if --client-id is used)"
    )
    parser.add_argument(
        "--password",
        help="Portal password (required if --username is used)"
    )
    
    # Optional metadata
    parser.add_argument(
        "--title",
        help="Title for the add-in item (optional)"
    )
    parser.add_argument(
        "--description",
        help="Description for the add-in item (optional)"
    )
    parser.add_argument(
        "--tags",
        help="Comma-separated tags for the add-in (optional)"
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    addin_file = Path(args.addin_file)
    if not addin_file.exists():
        print(f"ERROR: Add-in file not found at: {addin_file}")
        sys.exit(1)
    
    # Determine authentication method
    if args.client_id:
        if not args.client_secret:
            print("ERROR: --client-secret is required when using --client-id")
            sys.exit(1)
        auth_method = "token"
        auth_params = {
            "client_id": args.client_id,
            "client_secret": args.client_secret
        }
    else:
        if not args.password:
            print("ERROR: --password is required when using --username")
            sys.exit(1)
        auth_method = "username"
        auth_params = {
            "username": args.username,
            "password": args.password
        }
    
    print("=" * 50)
    print("  ArcGIS Online Publishing Script (Python)")
    print("=" * 50)
    print()
    print(f"Portal URL: {args.portal_url}")
    print(f"Item ID: {args.item_id}")
    print(f"Add-in file: {addin_file}")
    print(f"Authentication: {auth_method}")
    print(f"arcgis-python-api version: {arcgis_version}")
    print()
    
    try:
        # Step 1: Connect to ArcGIS Online
        print("🔐 Connecting to ArcGIS Online...")
        
        if auth_method == "token":
            # OAuth2 app client authentication
            gis = GIS(
                url=args.portal_url,
                client_id=auth_params["client_id"],
                client_secret=auth_params["client_secret"]
            )
        else:
            # Username/password authentication
            gis = GIS(
                url=args.portal_url,
                username=auth_params["username"],
                password=auth_params["password"]
            )
        
        print(f"✅ Connected as: {gis.users.me.username}")
        print()
        
        # Step 2: Get the item
        print(f"📦 Getting item {args.item_id}...")
        item = gis.content.get(args.item_id)
        
        if not item:
            print(f"ERROR: Item {args.item_id} not found or you don't have access to it")
            sys.exit(1)
        
        print(f"✅ Found item: {item.title}")
        print()
        
        # Step 3: Update the item with the new file
        print("📤 Uploading add-in file...")
        
        # Update the item with the new file
        update_result = item.update(
            data=str(addin_file),
            file_type="addIn"
        )
        
        if update_result:
            print("✅ File uploaded successfully")
        else:
            print("ERROR: File upload failed")
            sys.exit(1)
        
        # Step 4: Update metadata if provided
        metadata_updates = {}
        if args.title:
            metadata_updates["title"] = args.title
        if args.description:
            metadata_updates["description"] = args.description
        if args.tags:
            # Convert comma-separated tags to list
            tag_list = [tag.strip() for tag in args.tags.split(",")]
            metadata_updates["tags"] = tag_list
        
        if metadata_updates:
            print()
            print("📝 Updating item metadata...")
            try:
                item.update(metadata_updates)
                print("✅ Metadata updated successfully")
            except Exception as e:
                print(f"⚠️  Metadata update failed: {e}")
                # Don't fail the whole process if metadata update fails
        
        print()
        print("✅ Successfully published to ArcGIS Online!" + "\033[92m")
        print(f"   Item URL: {args.portal_url}/home/item.html?id={args.item_id}")
        print("\033[0m")
        print()
        
        sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: Failed to publish to ArcGIS Online: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

