#!/bin/sh

VERSION_FILE='torf/_version.py'
CHANGELOG_FILE='CHANGELOG'

error() {
    echo "$1" >&2
}


if ! twine -h >/dev/null; then
    error 'This script needs twine to upload to pypi.org.'
    exit 1
fi

if [ -n "$*" ]; then
    error "Usage: $0"
    exit 1
fi


get_new_version() {
    if [ ! -r "$VERSION_FILE" ]; then
        error "Unreadable VERSION_FILE: $VERSION_FILE"
        exit 1
    else
        grep "__version__[[:space:]]*=[[:space:]]*" "$VERSION_FILE" | \
            cut -d ' ' -f 3 | tr -d '"' | tr -d "'"
    fi
}

get_release_notes() {
    awk 'BEGIN {RS="\n\n\n"; FS="\n\n\n";} {print $1; exit}' "$CHANGELOG_FILE" \
        | tail -n +2 \
        | sed 's/^  //'
}

assert_pwd_is_git_repo() {
    if ! git status >/dev/null; then
        error "You are not in a git repository."
        exit 1
    fi
}

assert_working_dir_clean() {
    if ! git diff --quiet; then
        error "There are uncommitted changes:"
        git status --short --untracked-files=no
        exit 1
    fi
}

assert_new_version_does_not_exist() {
    if git show-ref --quiet "refs/tags/v$NEW_VERSION"; then
        error "Version $NEW_VERSION already exists:"
        git show-ref "v$NEW_VERSION"
        exit 1
    fi
}

assert_branch_is_master() {
    branch=$(git branch --no-color | grep '^*' | cut -d ' ' -f 2)
    if [ "$branch" != 'master' ]; then
        error 'You should release from master branch.'
        exit 1
    fi
}


NEW_VERSION="$(get_new_version)"
RELEASE_NOTES="$(get_release_notes)"
assert_pwd_is_git_repo
assert_working_dir_clean
assert_branch_is_master
assert_new_version_does_not_exist

make test


# Check if guessed version is correct
read -p "Confirm new version with return, CTRL-C to abort: $NEW_VERSION" key
[ "$key" != '' ] && exit 1

# Verify latest changelog header
new_header="$(date +%Y-%m-%d) $NEW_VERSION"
if [ "$(head -n1 "$CHANGELOG_FILE")" != "$new_header" ]; then
    error "Newest $CHANGELOG_FILE entry doesn't have expected header: $new_header"
    exit 1
fi


# Verify release notes
echo "Found the following release notes:"
echo "======================================================================"
echo "$RELEASE_NOTES"
echo "======================================================================"
read -p "Confirm release notes with return, CTRL-C to abort: " key
[ "$key" != '' ] && exit 1


# Verify user has a brain
read -p 'To make the release enter the release version again: ' version_check
echo
if [ "$version_check" != "$NEW_VERSION" ]; then
    error "You are obviously confused and in no shape to make a release."
    exit 1
fi


git tag -a "v$NEW_VERSION" -m "Version $NEW_VERSION" -m "$(get_release_notes)"
git push --follow-tags
make clean && python3 setup.py --quiet sdist
twine upload dist/*
make clean
