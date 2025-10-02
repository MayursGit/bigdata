#!/bin/bash
# -------------------------------
# Cross-platform Git setup script
# -------------------------------

# 1. Configure Git line endings
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    echo "Configuring Git for Windows (CRLF)..."
    git config --global core.autocrlf true
    git config --global credential.helper manager-core
else
    echo "Configuring Git for Linux (LF)..."
    git config --global core.autocrlf input
    git config --global credential.helper cache
fi

# 2. Create .gitattributes file
cat > .gitattributes <<EOL
# Default text handling
* text=auto

# Scripts
*.sh text eol=lf
*.bat text eol=crlf
*.py text eol=lf

# Config and documentation
*.md text eol=lf
*.json text eol=lf
*.yml text eol=lf
*.yaml text eol=lf

# Binary files
*.png binary
*.jpg binary
*.jpeg binary
*.gif binary
*.zip binary
*.jar binary
EOL

# 3. Stage and commit .gitattributes if not already committed
git add .gitattributes
git commit -m "Add cross-platform .gitattributes"

echo "✅ Git cross-platform setup complete!"
