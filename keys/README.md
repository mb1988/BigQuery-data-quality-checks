# ⚠️ CRITICAL: This directory contains sensitive credentials

**DO NOT COMMIT ANY FILES IN THIS DIRECTORY TO GIT**

This directory is protected by .gitignore and should contain:
- google-service-account.json (your GCP service account key)
- Any other API keys or credentials

## Security Checklist

✅ The keys/ directory is in .gitignore
✅ Never share files from this directory
✅ Rotate credentials annually
✅ Use environment variables to reference these files (.env)

## If You Accidentally Committed Keys

1. **IMMEDIATELY** rotate/delete the exposed credentials in Google Cloud Console
2. Remove from git history:
   ```bash
   git filter-branch --force --index-filter \
     "git rm --cached --ignore-unmatch keys/*" \
     --prune-empty --tag-name-filter cat -- --all
   ```
3. Force push: `git push origin --force --all`
4. Contact your security team

## Proper Usage

Reference the key in your .env file:
```
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/keys/google-service-account.json
```

Never hardcode paths in your code.

