# ArcGIS Online Publishing Setup Guide

This guide explains how to set up automatic publishing to ArcGIS Online (AGOL) when creating releases.

## Overview

The automated publishing system uploads your ArcGIS Pro add-in to ArcGIS Online automatically when you create a production release tag. This eliminates the need to manually upload files after each release.

## Authentication Methods

**Important**: For automated publishing from GitHub Actions, you need **app authentication** (client credentials flow), NOT user authentication. The guide at https://developers.arcgis.com/documentation/security-and-authentication/user-authentication/oauth-credentials-user/ is for user authentication, which requires interactive sign-in. For automated publishing, follow the steps below for app authentication.

You can use one of two authentication methods:

### Option 1: OAuth2 App Client Token (Recommended)

This is the recommended method as it's more secure and doesn't require storing your personal password. This uses **app authentication** (client credentials flow), not user authentication.

**Steps:**

1. **Create OAuth Credentials for App Authentication in ArcGIS Online:**
   - Log in to ArcGIS Online: https://www.arcgis.com
   - Go to **Content** → **My content** → **New item**
   - In the "New item" modal, click **Developer credentials** (the option with the key/code icon)
   - You'll see a "Create developer credentials" screen with "Select credential type"
   - Click on the **OAuth 2.0 credentials** card (or click "Next" if it's the only option)
   - **Important**: On the next screen, you'll see a comparison between "For user authentication" and "For app authentication"
   - Choose **App authentication** (not user authentication) - this uses the client credentials flow
   - The comparison table shows: "For app authentication" is for "public applications that do not require your users to sign in and generate access tokens server-side" - this is what you need!
   - On the configuration screen that follows:
     - **Redirect URLs**: Add `urn:ietf:wg:oauth:2.0:oob` (required by UI, but won't be used for app authentication/client credentials flow)
       - Type the URL in the field and click "+ Add"
       - This is a placeholder URL for out-of-band authentication flows
     - **Referrer URLs**: Leave blank (optional)
     - **Application environment**: Select **"Server"** (this is for server-side automation from GitHub Actions)
     - **URL**: Leave as default (`https://`) or blank
   - On the "Item details" screen:
     - **Title** (required): Enter a name like `GitHub Actions Publisher` or `AGOL Publishing Credentials`
     - **Folder**: Leave as default (your user folder)
     - **Tags** (optional): Add tags like `GitHub Actions`, `Automation`, `CI/CD`
     - **Summary** (optional): Add a brief description like "OAuth credentials for automated publishing of ArcGIS Pro add-in to ArcGIS Online from GitHub Actions"
   - Click **Create**
   - After creation, you'll be taken to the item Overview page
   - **Important**: Click the **"Settings"** tab, then click **"Register application"** button
   - Fill in the registration form:
     - **Redirect URLs**: Add `urn:ietf:wg:oauth:2.0:oob` or `https://localhost` (required by UI)
     - **Application environment**: Select **"Server"**
     - **URL**: Leave as default (`https://`) or blank
   - Click **"Register"**
   - **Important**: After registering, you'll see the **Client ID** and **Client Secret** on the Application page - copy both immediately!
     - Click the eye icon to reveal the Client Secret
     - Use the clipboard icons to copy both values
   
   **Note**: If you don't see "Developer credentials" as an option, you may need to:
   - Check that your account has the "Generate API keys" privilege
   - Contact your ArcGIS Online administrator to enable developer credentials
   
   **Reference**: For more details on app authentication, see: https://developers.arcgis.com/documentation/security-and-authentication/app-authentication/

2. **Add GitHub Secrets:**
   - Go to your GitHub repository
   - Navigate to **Settings** → **Secrets and variables** → **Actions**
   - Click **New repository secret** and add:
     - `AGOL_CLIENT_ID` = Your OAuth2 Client ID
     - `AGOL_CLIENT_SECRET` = Your OAuth2 Client Secret
     - `AGOL_ITEM_ID` = The item ID of your add-in in AGOL (see below)

### Option 2: Username/Password

If you prefer to use your ArcGIS Online username and password:

1. **Add GitHub Secrets:**
   - Go to your GitHub repository
   - Navigate to **Settings** → **Secrets and variables** → **Actions**
   - Click **New repository secret** and add:
     - `AGOL_USERNAME` = Your ArcGIS Online username
     - `AGOL_PASSWORD` = Your ArcGIS Online password
     - `AGOL_ITEM_ID` = The item ID of your add-in in AGOL (see below)

## Finding Your Item ID

The Item ID is the unique identifier for your add-in in ArcGIS Online:

1. Go to your add-in item page in ArcGIS Online
2. Look at the URL: `https://www.arcgis.com/home/item.html?id=**8293d1220b7848848ce316b4fa3263b5**`
3. The Item ID is the part after `id=` (e.g., `8293d1220b7848848ce316b4fa3263b5`)

## Optional Configuration

You can also set these optional secrets for more control:

- `AGOL_PORTAL_URL` - Custom portal URL (default: `https://www.arcgis.com`)
- `AGOL_TITLE` - Custom title for the add-in item
- `AGOL_DESCRIPTION` - Custom description for the add-in item
- `AGOL_TAGS` - Comma-separated tags (default: `ArcGIS Pro,Add-in,GeoParquet`)

## How It Works

1. **Create a production release tag** (e.g., `v0.1.4`)
2. **GitHub Actions automatically:**
   - Updates Config.daml version
   - Builds the add-in
   - Creates GitHub release
   - **Publishes to ArcGIS Online** (if credentials are configured)

## Testing

To test the AGOL publishing:

1. Ensure all required secrets are set
2. Create a test production tag: `git tag v0.1.4-test && git push origin v0.1.4-test`
3. Monitor the workflow in GitHub Actions
4. Check that the add-in was updated in ArcGIS Online

## Troubleshooting

### Publishing Step is Skipped

If the publishing step is skipped, check:
- Are the required secrets set? (Either OAuth2 credentials OR username/password)
- Is it a production tag? (Dev tags with `-dev` suffix won't publish)
- Check the workflow logs for error messages

### Authentication Fails

- **OAuth2**: Verify Client ID and Secret are correct
- **Username/Password**: Verify credentials are correct and account is active
- Check that the app/client has permission to update items

### Upload Fails (403 Permission Error)

If you see a "403 - You do not have permissions" error:

**For OAuth2 App Client Credentials:**
- OAuth2 app client credentials may not have permission to update items by default
- **Solution 1**: Grant permissions to the OAuth2 app:
  1. Go to ArcGIS Online → Content → Your OAuth credentials item
  2. Check the Settings tab for permission/scopes configuration
  3. Ensure the app has "Content: Update" or similar permissions
- **Solution 2**: Use username/password authentication instead (recommended):
  - OAuth2 app client credentials have limited permissions
  - Username/password authentication has full user permissions
  - Set `AGOL_USERNAME` and `AGOL_PASSWORD` in GitHub Secrets
  - Remove `AGOL_CLIENT_ID` and `AGOL_CLIENT_SECRET` (or leave them empty)
- **Solution 3**: Ensure the item is owned/shared with the app:
  - The add-in item must be accessible to the OAuth2 app
  - Check item sharing settings in ArcGIS Online

**For Username/Password:**
- Verify the Item ID is correct
- Check that you have permission to update the item
- Ensure the item exists in ArcGIS Online
- Check workflow logs for specific error messages

## Security Notes

- **Never commit secrets to the repository**
- Use GitHub Secrets for all sensitive information
- OAuth2 app client tokens are preferred over username/password
- Consider using a dedicated service account for automation

## Example Workflow

```bash
# 1. Set up secrets in GitHub (one-time setup)
# 2. Create a release tag
git tag v0.1.4
git push origin v0.1.4

# 3. GitHub Actions automatically:
#    - Updates version
#    - Builds add-in
#    - Creates GitHub release
#    - Publishes to AGOL ✅
```

## Support

For issues or questions:
- Check the workflow logs in GitHub Actions
- Review the PowerShell script: `.github/workflows/publish-agol.ps1`
- Open an issue in the repository

