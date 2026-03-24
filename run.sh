#!/bin/bash
echo "================================"
echo "  Collaby - NYC Listings Scout"
echo "================================"
echo

if command -v python3 &>/dev/null; then
    python3 scrape.py
elif command -v python &>/dev/null; then
    python scrape.py
else
    echo "Python is not installed."
    echo ""
    echo "Mac:     brew install python"
    echo "         or download from https://www.python.org/downloads/"
    echo "Linux:   sudo apt install python3"
    echo ""
    echo "Then run this again."
    exit 1
fi
