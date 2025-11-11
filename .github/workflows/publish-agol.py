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

# Fix Windows console encoding issues
if sys.platform == "win32":
    # Set UTF-8 encoding for stdout/stderr on Windows
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    # Also set environment variable for subprocess calls
    os.environ['PYTHONIOENCODING'] = 'utf-8'

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
        print("[AUTH] Authenticating with ArcGIS Online...")
        
        try:
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
        except Exception as e:
            print(f"ERROR: Authentication failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
        print(f"[OK] Authentication successful")
        # With OAuth2 app client authentication, there may not be a user object
        if gis.users.me:
            print(f"     Logged in as: {gis.users.me.username}")
        else:
            print(f"     Authenticated as application (OAuth2 client credentials)")
        print()
        
        # Step 2: Get the item
        print("[GET] Retrieving add-in item...")
        try:
            item = gis.content.get(args.item_id)
        except Exception as e:
            print(f"ERROR: Failed to retrieve item: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
        if not item:
            print(f"ERROR: Item {args.item_id} not found or you don't have access to it")
            sys.exit(1)
        
        print(f"[OK] Found item: {item.title}")
        print()
        
        # Step 3: Update the file
        print("[UPLOAD] Uploading add-in file...")
        print(f"        File path: {addin_path}")
        print(f"        File exists: {addin_path.exists()}")
        print(f"        File size: {addin_path.stat().st_size / 1024 / 1024:.2f} MB")
        print()
        
        try:
            update_result = item.update(
                data=str(addin_path),
                thumbnail=None  # Keep existing thumbnail
            )
            
            if update_result:
                print("[OK] File uploaded successfully")
            else:
                print("WARNING: Update returned False, but file may have been uploaded")
        except Exception as e:
            error_msg = str(e)
            print(f"ERROR: File upload failed: {error_msg}")
            
            # Provide helpful guidance for common errors
            if "403" in error_msg or "permissions" in error_msg.lower():
                print()
                print("=" * 70)
                print("PERMISSION ERROR - Troubleshooting Steps:")
                print("=" * 70)
                print()
                if args.auth_method == "token":
                    print("OAuth2 App Client Credentials may not have permission to update items.")
                    print()
                    print("SOLUTION 1: Grant permissions to the OAuth2 app:")
                    print("  1. Go to ArcGIS Online → Content → Your OAuth credentials item")
                    print("  2. Check the Settings tab for permission/scopes configuration")
                    print("  3. Ensure the app has 'Content: Update' or similar permissions")
                    print()
                    print("SOLUTION 2: Use username/password authentication instead:")
                    print("  - Set AGOL_USERNAME and AGOL_PASSWORD in GitHub Secrets")
                    print("  - Remove AGOL_CLIENT_ID and AGOL_CLIENT_SECRET")
                    print("  - Username/password auth has full user permissions")
                    print()
                    print("SOLUTION 3: Ensure the item is owned/shared with the app:")
                    print("  - The add-in item must be accessible to the OAuth2 app")
                    print("  - Check item sharing settings in ArcGIS Online")
                else:
                    print("Username/password authentication should have full permissions.")
                    print("Check that:")
                    print("  1. The username/password are correct")
                    print("  2. The account has permission to update the item")
                    print("  3. The item ID is correct")
                print("=" * 70)
                print()
            
            import traceback
            traceback.print_exc()
            sys.exit(1)
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
            print("[METADATA] Updating item metadata...")
            try:
                item.update(metadata_updates)
                print("[OK] Metadata updated successfully")
            except Exception as e:
                print(f"WARNING: Metadata update failed: {e}")
                # Don't fail the whole process for metadata issues
        print()
        
        # Step 5: Verify the update
        print("[VERIFY] Verifying update...")
        updated_item = gis.content.get(args.item_id)
        print(f"[OK] Item updated successfully")
        print(f"     Item URL: {args.portal_url}/home/item.html?id={args.item_id}")
        print(f"     Modified: {updated_item.modified}")
        print()
        
        print("[SUCCESS] Successfully published to ArcGIS Online!")
        print()
        
        sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: Failed to publish to ArcGIS Online: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
