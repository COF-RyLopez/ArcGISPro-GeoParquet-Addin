# Automated Versioning & Release Guide

**Quick Summary**: This project now has fully automated version management. Just create a git tag and push it - everything else happens automatically!

---

## 📋 Table of Contents

1. [What Was Fixed](#what-was-fixed)
2. [How It Works Now](#how-it-works-now)
3. [Creating a Release](#creating-a-release)
4. [Testing with Dev Branch](#testing-with-dev-branch)
5. [Build System Improvements](#build-system-improvements)
6. [Testing](#testing)
7. [Troubleshooting](#troubleshooting)

---

## What Was Fixed

### The Problem (Issue #5)

- **Config.daml** showed version `0.1.0`
- **GitHub Release** showed `v0.1.2`
- **Users couldn't tell** which version they had installed

**Root Cause**: Version in Config.daml had to be manually updated, leading to version drift.

### The Solution

✅ **Automated Version Management**
- Config.daml is automatically updated from git tags
- Version is automatically committed back to main branch
- All versions stay synchronized

✅ **Build System Improvements**
- Smart conditional references that detect the environment
- Local development uses installed ArcGIS Pro
- CI/CD automatically uses NuGet package
- No more fragile csproj modification scripts

✅ **Current Status**: Config.daml updated to `0.1.2` and synchronized with latest release

---

## How It Works Now

### Automated Workflow

```
Developer Creates Tag (v0.1.3)
         ↓
    Push to GitHub
         ↓
GitHub Actions Automatically:
  1. Updates Config.daml to 0.1.3
  2. Builds add-in (.esriAddInX)
  3. Commits version back to main [skip ci]
  4. Creates GitHub Release
  5. Uploads release package
         ↓
   All versions match!
```

### Key Features

- **Zero manual version management** - Just create and push a tag
- **Main branch stays synchronized** - Version changes committed automatically
- **No infinite loops** - Uses `[skip ci]` in commit message
- **Smart builds** - Automatically detects local vs CI/CD environment

---

## Creating a Release

### Prerequisites

- All code changes committed and pushed to `main`
- Code tested locally in ArcGIS Pro
- Decide on version number (see [Semantic Versioning](#semantic-versioning))

### Step-by-Step Process

#### 1. Create and Push a Version Tag

```bash
# For a patch release (bug fixes): 0.1.2 → 0.1.3
git tag v0.1.3

# For a minor release (new features): 0.1.x → 0.2.0
git tag v0.2.0

# For a major release (breaking changes): 0.x.y → 1.0.0
git tag v1.0.0

# Push the tag to trigger the release
git push origin v0.1.3
```

#### 2. Monitor GitHub Actions

Go to: https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin/actions

Watch the "Build ArcGIS Pro GeoParquet Add-in" workflow. It will:
- ✅ Update Config.daml with the version
- ✅ Build the add-in
- ✅ Commit the version back to main
- ✅ Create the release

#### 3. Review and Publish the Release

1. Go to: https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin/releases
2. Find your new release (will be in "Draft" mode)
3. Edit the release notes if desired
4. Click **Publish Release**

#### 4. Verify Everything Worked

```bash
# Pull the version update that was committed back
git pull origin main

# Check that Config.daml has the correct version
grep 'version=' Config.daml
# Should show: version="0.1.3"
```

**Done!** Your release is complete and all versions are synchronized.

### Semantic Versioning

Follow [Semantic Versioning](https://semver.org/) format: `MAJOR.MINOR.PATCH`

- **PATCH** (0.1.2 → 0.1.3): Bug fixes, small improvements
- **MINOR** (0.1.x → 0.2.0): New features, backward compatible
- **MAJOR** (0.x.y → 1.0.0): Breaking changes

### Pre-Release Checklist

Before creating a release tag:

- [ ] All tests pass locally
- [ ] Code builds successfully in Release configuration
- [ ] Add-in works correctly in ArcGIS Pro 3.5+
- [ ] README.md is up to date (if needed)
- [ ] No sensitive data (keys, passwords) in code
- [ ] Breaking changes are documented (if any)

---

## Testing with Dev Branch

### Why Use a Dev Branch?

The `dev` branch allows you to **test workflow changes** before they go to production:

- ✅ Test new workflow features
- ✅ Verify versioning logic
- ✅ Check build system changes
- ✅ No risk to production releases
- ✅ No commits back to main

### How to Use Dev Branch

#### 1. Create and Push Dev Branch (First Time Only)

```bash
# Create dev branch from main
git checkout -b dev main

# Push to GitHub
git push -u origin dev
```

#### 2. Make Changes and Test

```bash
# Make your workflow changes
git add .github/workflows/build.yaml
git commit -m "test: experiment with workflow changes"

# Push to dev branch
git push origin dev

# This triggers the workflow on dev branch
```

#### 3. Test with Dev Tags

```bash
# Create a dev tag for testing
git tag v0.1.3-dev

# Push the dev tag
git push origin v0.1.3-dev
```

**What happens with dev tags:**
- ✅ Config.daml gets updated (without -dev suffix)
- ✅ Build runs and creates artifacts
- ✅ Creates a **pre-release** on GitHub (marked as dev)
- ❌ Does NOT commit version back to main
- ❌ Does NOT create a production release

#### 4. Verify the Dev Build

1. Go to: https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin/actions
2. Watch the workflow run on dev branch
3. Check the pre-release: https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin/releases
4. Download and test the dev build

#### 5. Clean Up Dev Releases

```bash
# Delete the dev tag when done testing
git push origin --delete v0.1.3-dev
git tag -d v0.1.3-dev

# Delete the pre-release on GitHub (via releases page)
```

#### 6. Merge to Main When Ready

```bash
# Switch back to main
git checkout main

# Merge dev changes
git merge dev

# Push to main
git push origin main

# Now create a production release
git tag v0.1.3
git push origin v0.1.3
```

### Dev vs Production Comparison

| Aspect | Dev Branch/Tags | Production (Main) |
|--------|----------------|-------------------|
| **Trigger** | Push to `dev` or `v*-dev` tags | Push to `main` or `v*` tags |
| **Version Update** | ✅ Yes (without -dev) | ✅ Yes |
| **Build** | ✅ Yes | ✅ Yes |
| **Commit Back** | ❌ No | ✅ Yes (to main) |
| **Release Type** | Pre-release | Production release |
| **Safe to Test** | ✅ Yes | ⚠️ Creates production release |

### Best Practices

1. **Always test workflow changes on dev first**
   - Prevents breaking production releases
   - Validates changes before merging

2. **Use descriptive dev tag names**
   ```bash
   v0.1.3-dev       # Good
   v0.1.3-dev-test  # Better (shows it's a test)
   ```

3. **Clean up dev releases**
   - Delete dev tags after testing
   - Remove pre-releases from GitHub

4. **Merge to main only when confident**
   - All tests pass
   - Workflow behaves as expected
   - Ready for production

---

## Build System Improvements

### Conditional References

The project now uses smart conditional references that automatically detect the environment:

**When ArcGIS Pro is installed locally** (development):
```xml
<ItemGroup Condition="Exists('C:\Program Files\ArcGIS\Pro\bin\ArcGIS.Core.dll')">
  <Reference Include="ArcGIS.Core">
    <HintPath>C:\Program Files\ArcGIS\Pro\bin\ArcGIS.Core.dll</HintPath>
  </Reference>
</ItemGroup>
```

**When ArcGIS Pro is NOT installed** (CI/CD):
```xml
<PackageReference Include="Esri.ArcGISPro.Extensions30" Version="3.5.0.57366"
                  Condition="!Exists('C:\Program Files\ArcGIS\Pro\bin\ArcGIS.Core.dll')" />
```

### Benefits

- ✅ **For Developers**: Use your local ArcGIS Pro installation (no changes needed)
- ✅ **For CI/CD**: Automatically uses the official Esri NuGet package
- ✅ **No manual modification**: Project file automatically adapts to the environment
- ✅ **Follows best practices**: Uses Esri's official recommendations

### What Was Removed

- ❌ `.github/workflows/update-csproj.ps1` - No longer needed
- ❌ Manual NuGet installation step in workflow
- ❌ Fragile string replacement hacks

---

## Testing

### Test the Version Update Script Locally

```bash
# Update Config.daml to a test version
pwsh .github/workflows/update-version.ps1 -Version "0.1.3" -ConfigPath "Config.daml"

# Verify the change
cat Config.daml | Select-String "version="
```

### Test a Complete Release (Recommended Before v0.1.3)

```bash
# 1. Create a test tag
git tag v0.1.3-test

# 2. Push the tag
git push origin v0.1.3-test

# 3. Monitor GitHub Actions
# Watch the workflow at: https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin/actions

# 4. Verify everything:
# - Config.daml was updated in the workflow logs
# - Build succeeded
# - Version was committed back to main branch
# - Commit message includes [skip ci]
# - No infinite workflow loops
# - GitHub release was created

# 5. Check the main branch
git pull origin main
grep 'version=' Config.daml
# Should show: version="0.1.3-test"

# 6. Clean up the test
# - Delete the test release on GitHub (releases page)
# - Delete the remote tag: git push origin --delete v0.1.3-test
# - Delete the local tag: git tag -d v0.1.3-test
```

---

## Troubleshooting

### Issue: Workflow fails at "Update Config.daml version from tag"

**Cause**: PowerShell script error or XML parsing issue

**Solution**:
```bash
# Test the script locally to see the error
pwsh .github/workflows/update-version.ps1 -Version "0.1.3" -ConfigPath "Config.daml"

# Check Config.daml for XML syntax errors
cat Config.daml
```

### Issue: Version in ArcGIS Pro doesn't match release

**Cause**: Old add-in cached or not fully uninstalled

**Solution**:
1. Completely uninstall the add-in in ArcGIS Pro
2. Delete add-in cache:
   - Path: `C:\Users\<YourName>\AppData\Local\ESRI\ArcGISPro\AssemblyCache`
3. Restart ArcGIS Pro completely
4. Reinstall the new version from the release `.zip` file

### Issue: Version wasn't committed back to main

**Cause**: Permission issue or git configuration error

**Solution**:
1. Check that the workflow has `contents: write` permission (already set in `build.yaml`)
2. Check GitHub Actions logs for error messages
3. Verify that `fetch-depth: 0` is set in checkout step (already configured)

### Issue: Release created but version is wrong

**Cause**: Tag doesn't match expected format

**Solution**:
```bash
# Delete the incorrect tag
git push origin --delete v0.1.3
git tag -d v0.1.3

# Recreate with correct format (must start with 'v')
git tag v0.1.3

# Push again
git push origin v0.1.3
```

### Issue: Infinite workflow loop

**Cause**: Commit message doesn't include `[skip ci]`

**Solution**: This is already handled in the workflow script. If you see this:
1. Check the workflow file for the commit message format
2. Verify `[skip ci]` is in the commit message template
3. Stop the running workflows manually in GitHub Actions

### Issue: Build fails with reference errors

**Cause**: Conditional references not working or NuGet package missing

**Solution**:
1. For local builds: Verify ArcGIS Pro 3.5 is installed
2. For CI/CD: Check that NuGet restore completed successfully
3. Check GitHub Actions logs for specific error messages

---

## File Reference

### Files Modified

- **`.github/workflows/build.yaml`** - Automated versioning and build workflow
- **`DuckDBGeoparquet.csproj`** - Conditional ArcGIS Pro references
- **`Config.daml`** - Version updated to 0.1.2
- **`README.md`** - Version history updated

### Files Created

- **`.github/workflows/update-version.ps1`** - PowerShell script to update Config.daml
- **`AUTOMATED_VERSIONING.md`** - This documentation file

### Files Removed

- **`.github/workflows/update-csproj.ps1`** - Obsolete (replaced by conditional references)

---

## Quick Reference

### Production Release

```bash
# Create and push production release
git tag v0.1.3 && git push origin v0.1.3
```

That's it! Everything else is automatic.

### Dev/Test Release

```bash
# Test workflow changes on dev branch
git checkout dev
git push origin dev

# Or create a dev tag for testing
git tag v0.1.3-dev && git push origin v0.1.3-dev
```

Safe testing - no commits to main, creates pre-release only.

### Check Current Version

```bash
# In Config.daml
grep 'version=' Config.daml

# Latest git tag
git describe --tags --abbrev=0

# Latest GitHub release
gh release list | head -n 1
```

### Version Update Flow

```
Tag → GitHub Actions → Update Config.daml → Build → Commit to main → Create Release
```

---

## Future Enhancements (Optional)

### 1. Automatic AGOL Publishing
Automatically publish releases to ArcGIS Online using Portal REST API.

**Requirements**:
- Add AGOL credentials to GitHub Secrets
- Create PowerShell functions for AGOL API
- Add publishing step to workflow

**Benefit**: Fully automated release from code to marketplace

### 2. CHANGELOG.md Generation
Automatically generate changelog from commit messages and PRs.

**Tools**:
- `github-changelog-generator`
- `conventional-changelog`
- GitHub API for PR notes

**Benefit**: Automatic, consistent release notes

### 3. Version in AssemblyInfo
Embed version in compiled DLL metadata.

```xml
<PropertyGroup>
  <Version>0.1.3</Version>
  <AssemblyVersion>0.1.3.0</AssemblyVersion>
  <FileVersion>0.1.3.0</FileVersion>
</PropertyGroup>
```

**Benefit**: Programmatically check version at runtime

---

## Summary

### What Changed

**Before**: Manual version updates → version drift → user confusion

**After**: Automatic version management → everything synchronized → professional releases

### Key Benefits

✅ **For Developers**:
- Release process: 6 manual steps → 2 commands
- Zero manual version management
- Main branch always synchronized

✅ **For CI/CD**:
- Robust, maintainable workflow
- Follows Esri's best practices
- No fragile hacks

✅ **For Users**:
- Version shown in ArcGIS Pro matches GitHub release
- Clear release history
- Professional experience

### Next Steps

1. Test the solution with the next release (v0.1.3)
2. Consider optional enhancements (AGOL publishing, etc.)
3. Keep this documentation updated

---

**Last Updated**: 2025-11-08
**Status**: ✅ Production Ready
**Issue**: [#5 - Incorrect version shown in Add-in](https://github.com/COF-RyLopez/ArcGISPro-GeoParquet-Addin/issues/5)

For questions or issues, open a GitHub issue with:
- Steps to reproduce
- Error messages
- GitHub Actions workflow logs
