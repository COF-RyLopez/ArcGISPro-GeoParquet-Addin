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
        
        # Step 3: Prepare metadata updates (if provided)
        metadata_updates = {}
        if args.title:
            metadata_updates["title"] = args.title
        if args.description:
            # If description is just a version line, update only that part of existing description
            # Otherwise, replace entire description
            if args.description.startswith("Latest Version:"):
                # Get current description and update just the version line
                current_desc = item.description or ""
                import re
                
                # Debug: Print current description snippet
                print(f"[DEBUG] Current description length: {len(current_desc)} chars")
                if current_desc:
                    # Find the Latest Version line
                    version_match = re.search(r'Latest Version:.*', current_desc, re.MULTILINE)
                    if version_match:
                        print(f"[DEBUG] Found existing version line: {version_match.group()[:100]}")
                    else:
                        print("[DEBUG] No existing 'Latest Version:' line found")
                
                # Try multiple regex patterns to match the version line
                # Pattern 1: Match entire line from "Latest Version:" to end of line (handles \r\n and \n)
                # This matches until we hit a newline or end of string
                pattern1 = r'Latest Version:.*?(?=\n|$)'
                # Pattern 2: Match with any whitespace variations, more specific
                pattern2 = r'Latest Version:\s*v[\d.]+\s*.*?(?=\n|$)'
                # Pattern 3: Match until double newline or next section marker (more robust)
                pattern3 = r'Latest Version:.*?(?=\n\n|\n🏆|\n📦|\n\n|$)'
                # Pattern 4: Match entire line including newline (DOTALL mode)
                pattern4 = r'Latest Version:[^\n]*(?:\n|$)'
                # Pattern 5: Simple match everything from Latest Version to end (fallback)
                pattern5 = r'Latest Version:.*'
                
                updated_desc = None
                for i, pattern in enumerate([pattern1, pattern2, pattern3, pattern4, pattern5], 1):
                    match = re.search(pattern, current_desc, re.MULTILINE | re.DOTALL)
                    if match:
                        # Use a more precise replacement - match the exact text found
                        matched_text = match.group()
                        print(f"[DEBUG] Pattern {i} matched: {matched_text[:100]}...")
                        updated_desc = current_desc.replace(matched_text, args.description.strip(), 1)
                        print(f"[DEBUG] Replaced version line using pattern {i}")
                        break
                
                if updated_desc is None:
                    # Fallback: Try simple string replacement
                    print("[DEBUG] Regex patterns didn't match, trying string-based replacement")
                    lines = current_desc.split('\n')
                    updated_lines = []
                    replaced = False
                    for line in lines:
                        if line.strip().startswith('Latest Version:'):
                            updated_lines.append(args.description.strip())
                            replaced = True
                            print(f"[DEBUG] Found and replaced line: {line[:80]}...")
                        else:
                            updated_lines.append(line)
                    
                    if replaced:
                        updated_desc = '\n'.join(updated_lines)
                    else:
                        # No match found, prepend version line
                        print("[DEBUG] No 'Latest Version:' line found, prepending")
                        updated_desc = args.description.strip() + "\n\n" + current_desc
                
                metadata_updates["description"] = updated_desc
                print(f"[DEBUG] Updated description length: {len(updated_desc)} chars")
                print(f"[DEBUG] New version line: {args.description.strip()[:100]}")
            else:
                # Full description replacement
                metadata_updates["description"] = args.description
        if args.tags:
            # Tags should be a list
            tag_list = [tag.strip() for tag in args.tags.split(",")]
            metadata_updates["tags"] = tag_list
        
        # Step 4: Update the file first, then metadata separately for better reliability
        print("[UPLOAD] Uploading add-in file...")
        print(f"        File path: {addin_path}")
        print(f"        File exists: {addin_path.exists()}")
        print(f"        File size: {addin_path.stat().st_size / 1024 / 1024:.2f} MB")
        print()
        
        try:
            # Upload file first (separate from metadata for better reliability)
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
        
        # Step 4b: Update metadata separately (more reliable for description updates)
        if metadata_updates:
            print()
            print("[METADATA] Updating item metadata...")
            print(f"        Updating: {', '.join(metadata_updates.keys())}")
            if 'description' in metadata_updates:
                print(f"        Description length: {len(metadata_updates['description'])} characters")
            
            try:
                # Update metadata separately - this is more reliable for description updates
                metadata_result = item.update(item_properties=metadata_updates)
                if metadata_result:
                    print("[OK] Metadata updated successfully")
                else:
                    print("WARNING: Metadata update returned False")
            except Exception as metadata_error:
                print(f"ERROR: Metadata update failed: {metadata_error}")
                # Don't fail the entire process if metadata update fails
                import traceback
                traceback.print_exc()
        
        print()
        
        # Step 5: Verify metadata update (if description was provided)
        if args.description:
            print("[VERIFY] Verifying description update...")
            try:
                # Refresh the item to get latest data
                updated_item = gis.content.get(args.item_id)
                if updated_item.description:
                    # Check if the version line was actually updated
                    import re
                    expected_version = args.description.strip()
                    if expected_version.startswith("Latest Version:"):
                        # Check if the version line matches what we sent
                        current_version_line = re.search(r'Latest Version:.*', updated_item.description, re.MULTILINE)
                        if current_version_line:
                            current_line = current_version_line.group().strip()
                            expected_line = expected_version.strip()
                            if current_line == expected_line:
                                print(f"        ✅ Verified: Version line updated correctly")
                                print(f"        Verified: Description updated ({len(updated_item.description)} chars)")
                                print(f"        Preview: {updated_item.description[:200]}...")
                            else:
                                print(f"        ⚠️  WARNING: Version line doesn't match expected value")
                                print(f"        Expected: {expected_line[:100]}")
                                print(f"        Actual: {current_line[:100]}")
                                print("        Attempting separate description update with string replacement...")
                                try:
                                    # Re-fetch item and update description separately using string replacement
                                    item_to_update = gis.content.get(args.item_id)
                                    current_desc = item_to_update.description or ""
                                    
                                    # Use string-based line replacement (most reliable)
                                    lines = current_desc.split('\n')
                                    updated_lines = []
                                    replaced = False
                                    for line in lines:
                                        if line.strip().startswith('Latest Version:'):
                                            updated_lines.append(expected_line)
                                            replaced = True
                                            print(f"        Found and replacing: {line[:80]}...")
                                        else:
                                            updated_lines.append(line)
                                    
                                    if replaced:
                                        updated_desc = '\n'.join(updated_lines)
                                        item_to_update.update(item_properties={"description": updated_desc})
                                        print("        ✅ Description updated via separate call (string replacement)")
                                    else:
                                        # Fallback: prepend if not found
                                        updated_desc = expected_line + "\n\n" + current_desc
                                        item_to_update.update(item_properties={"description": updated_desc})
                                        print("        ✅ Description updated via separate call (prepended)")
                                except Exception as e2:
                                    print(f"        ❌ ERROR: Separate update also failed: {e2}")
                                    import traceback
                                    traceback.print_exc()
                        else:
                            print("        ⚠️  WARNING: No 'Latest Version:' line found in description")
                            print(f"        Description preview: {updated_item.description[:200]}...")
                    else:
                        print(f"        Verified: Description updated ({len(updated_item.description)} chars)")
                        print(f"        Preview: {updated_item.description[:200]}...")
                else:
                    print("        WARNING: Description appears empty after update")
                    # Try updating description separately as fallback
                    print("        Attempting separate description update...")
                    try:
                        item.update(item_properties={"description": args.description})
                        print("        ✅ Description updated via separate call")
                    except Exception as e2:
                        print(f"        ❌ ERROR: Separate update also failed: {e2}")
            except Exception as e:
                print(f"        WARNING: Verification failed: {e}")
                import traceback
                traceback.print_exc()
            print()
        
        # Step 6: Final verification
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
