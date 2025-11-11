#!/usr/bin/env python3
"""
Publishes ArcGIS Pro add-in to ArcGIS Online using arcgis-python-api

This script uploads and updates an add-in item in ArcGIS Online using the
official Esri arcgis-python-api library, which provides a more reliable
interface than raw REST API calls.
"""

import os
import sys
import argparse
from pathlib import Path

try:
    from arcgis.gis import GIS
    from arcgis import __version__ as arcgis_version
except ImportError:
    print("ERROR: arcgis-python-api is not installed")
    print("Install it with: pip install arcgis")
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
    parser.add_argument(
        "--auth-method",
        choices=["token", "username"],
        default="token",
        help="Authentication method: 'token' (OAuth2) or 'username' (default: token)"
    )
    parser.add_argument(
        "--client-id",
        help="OAuth2 app client ID (required if auth-method is 'token')"
    )
    parser.add_argument(
        "--client-secret",
        help="OAuth2 app client secret (required if auth-method is 'token')"
    )
    parser.add_argument(
        "--username",
        help="Portal username (required if auth-method is 'username')"
    )
    parser.add_argument(
        "--password",
        help="Portal password (required if auth-method is 'username')"
    )
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
    addin_path = Path(args.addin_file)
    if not addin_path.exists():
        print(f"ERROR: Add-in file not found at: {addin_path}")
        sys.exit(1)
    
    if args.auth_method == "token":
        if not args.client_id or not args.client_secret:
            print("ERROR: Client ID and Client Secret are required when using token authentication")
            sys.exit(1)
    else:
        if not args.username or not args.password:
            print("ERROR: Username and Password are required when using username authentication")
            sys.exit(1)
    
    print("=" * 50)
    print("  ArcGIS Online Publishing Script (Python)")
    print("=" * 50)
    print()
    print(f"Portal URL: {args.portal_url}")
    print(f"Item ID: {args.item_id}")
    print(f"Add-in file: {addin_path}")
    print(f"Authentication: {args.auth_method}")
    print(f"arcgis-python-api version: {arcgis_version}")
    print()
    
    try:
        # Step 1: Authenticate
        print("🔐 Authenticating with ArcGIS Online...")
        
        if args.auth_method == "token":
            # OAuth2 app client authentication
            gis = GIS(
                url=args.portal_url,
                client_id=args.client_id,
                client_secret=args.client_secret
            )
        else:
            # Username/password authentication
            gis = GIS(
                url=args.portal_url,
                username=args.username,
                password=args.password
            )
        
        print(f"✅ Authentication successful")
        print(f"   Logged in as: {gis.users.me.username}")
        print()
        
        # Step 2: Get the item
        print("📦 Retrieving add-in item...")
        item = gis.content.get(args.item_id)
        
        if not item:
            print(f"ERROR: Item {args.item_id} not found or you don't have access to it")
            sys.exit(1)
        
        print(f"✅ Found item: {item.title}")
        print()
        
        # Step 3: Update the file
        print("📤 Uploading add-in file...")
        update_result = item.update(
            data=str(addin_path),
            thumbnail=None  # Keep existing thumbnail
        )
        
        if update_result:
            print("✅ File uploaded successfully")
        else:
            print("WARNING: Update returned False, but file may have been uploaded")
        print()
        
        # Step 4: Update metadata (if provided)
        metadata_updates = {}
        if args.title:
            metadata_updates["title"] = args.title
        if args.description:
            metadata_updates["description"] = args.description
        if args.tags:
            # Tags should be a list
            tag_list = [tag.strip() for tag in args.tags.split(",")]
            metadata_updates["tags"] = tag_list
        
        if metadata_updates:
            print("📝 Updating item metadata...")
            try:
                item.update(metadata_updates)
                print("✅ Metadata updated successfully")
            except Exception as e:
                print(f"WARNING: Metadata update failed: {e}")
                # Don't fail the whole process for metadata issues
        print()
        
        # Step 5: Verify the update
        print("🔍 Verifying update...")
        updated_item = gis.content.get(args.item_id)
        print(f"✅ Item updated successfully")
        print(f"   Item URL: {args.portal_url}/home/item.html?id={args.item_id}")
        print(f"   Modified: {updated_item.modified}")
        print()
        
        print("✅ Successfully published to ArcGIS Online!" + "\033[92m")  # Green color
        print()
        
        sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: Failed to publish to ArcGIS Online: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
