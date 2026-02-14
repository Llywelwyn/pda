<p align="center"></p><!-- spacer -->

<div align="center">
  <img src="https://raw.githubusercontent.com/llywelwyn/pda/master/docs/pda.png"
       alt="pda"
       width="200" />
</div>

<p align="center"></p><!-- spacer -->

<div align="center">
  <a href="https://github.com/llywelwyn/pda/actions" rel="nofollow">
    <img src="https://img.shields.io/github/actions/workflow/status/llywelwyn/pda/go.yml?branch=main"
         alt="build status"
         style="max-width:100%;">
  </a>
</div>

<p align="center"></p><!-- spacer -->

`pda!` is a command-line key-value store tool with:
- [templates](#templates) supporting arbitrary shell execution, conditionals, loops, more,
- [encryption](#encryption) at rest using [age](https://github.com/FiloSottile/age),
- Git-backed [version control](#git) with automatic syncing,
- [search and filtering](#filtering) by key, value, or store,
- plaintext exports in 7 different formats,
- support for all [binary data](#binary-data),
- expiring keys with a [time-to-live](#ttl),
- [read-only](#read-only) keys and [pinned](#pinned) entries,
- built-in [diagnostics](#doctor) and [configuration](#config),

and more, written in pure Go, and inspired by [skate](https://github.com/charmbracelet/skate) and [nb](https://github.com/xwmx/nb).

<p align="center"></p><!-- spacer -->

`pda!` stores key-value pairs natively as [newline-delimited JSON](https://en.wikipedia.org/wiki/JSON_streaming#Newline-delimited_JSON) files. Every store is plaintext, portable, and yours. There's no daemon, no cloud service, and no proprietary format. Keys are just lines in a JSON file; stores are just files in a directory. If you can `cat` a file, you can read your data without `pda!` installed.

Git versioning is built in. Enable auto-committing, pushing, and fetching in the [config](#config) to automatically version every change, or just run [`pda sync`](#sync) when you want to. Because the storage format is line-oriented plaintext, diffs are meaningful and merges are clean.

Go's [`text/template`](https://pkg.go.dev/text/template) engine is available on every value at retrieval time, turning simple key-value pairs into dynamic snippets with variables, environment lookups, shell execution, cross-references, and more.

<p align="center"></p><!-- spacer -->

<div align="center">
  <a href="https://github.com/llywelwyn/pda/actions" rel="nofollow">
    <img src="https://github.com/Llywelwyn/pda/blob/main/vhs/intro.gif"
         alt="pda demo"
         style="max-width:50%;">
  </a>
</div>

<p align="center"></p><!-- spacer -->

### Installation

<p>
  <sup>
    <a href="#overview">↑</a>
    <a href="#prerequisites">Prerequisites</a>&nbsp;·
    <a href="#build">Build</a>&nbsp;·
    <a href="#setting-up-shell-completion">Shell Completion</a>
  </sup>
</p>

#### Prerequisites

`pda` has no mandatory requirements outside of a shell to run it in. However, it is enhanced by other tools being installed.

- [go](https://go.dev) is needed for compiling the `pda` binary.
- [git](https://git-scm.com) enhances `pda` with [version control](#git).

#### Build

The easiest (and most universal) way to install `pda` is to use `go install` to build from source. The same command can be used to update.

```bash
go install github.com/llywelwyn/pda@latest

# Or from a spceific commit.
git clone https://github.com/llywelwyn/pda
cd pda
go install
```

[Arch Linux](https://archlinux.org) users can install and update `pda` from the [aur](https://aur.archlinux.org) with a package manager of choice. There are two packages available: `pda`, the latest stable release, and `pda-git`, which will install the latest commit to the main branch on this repository.

```bash
# Latest stable release
yay -S pda

# Latest commit
yay -S pda-git

# Updating
yay -Syu pda
```

#### Setting up Shell Completion

`pda` is built with [cobra](https://cobra.dev) and so comes with shell completions for bash, zsh, fish, and powershell.

```bash
# Bash
pda completion bash > /etc/bash_completion.d/pda

# Zsh
pda completion zsh > "${fpath[1]}/_pda"

# Fish
pda completion fish > ~/.config/fish/completions/pda.fish

# Powershell
pda completion powershell | Out-String | Invoke-Expression
```

Powershell users will need to manually add the above command to their profile; the given command will only instantiate `pda` for the current shell instance.

<p align="center"></p><!-- spacer -->

## Overview

<div align="center">
  <a href="#setting">Setting</a>&nbsp;·
  <a href="#getting">Getting</a>&nbsp;·
  <a href="#running">Running</a>&nbsp;·
  <a href="#listing">Listing</a>&nbsp;·
  <a href="#editing">Editing</a>&nbsp;·
  <a href="#moving--copying">Moving&nbsp;&&nbsp;Copying</a>&nbsp;·
  <a href="#removing">Removing</a>&nbsp;·
  <a href="#metadata">Metadata</a>&nbsp;·
  <a href="#ttl">TTL</a>&nbsp;·
  <a href="#encryption">Encryption</a>&nbsp;·
  <a href="#read-only">Read-Only</a>&nbsp;·
  <a href="#pinned">Pinned</a>&nbsp;·
  <a href="#stores">Stores</a>&nbsp;·
  <a href="#import--export">Import&nbsp;&&nbsp;Export</a>&nbsp;·
  <a href="#templates">Templates</a>&nbsp;·
  <a href="#filtering">Filtering</a>&nbsp;·
  <a href="#binary-data">Binary&nbsp;Data</a>
  <a href="#git">Git</a>&nbsp;·
  <a href="#identity">Identity</a>&nbsp;·
  <a href="#config">Config</a>&nbsp;·
  <a href="#environment">Environment</a>&nbsp;·
  <a href="#doctor">Doctor</a>&nbsp;·
  <a href="#help--version">Help&nbsp;&&nbsp;Version</a>
</div>

<p align="center"></p><!-- spacer -->

```bash
pda! MIT licensed. (c) 2025 Lewis Wynne

Usage:
  pda [command]

Key commands:
  copy         Make a copy of a key
  edit         Edit a key's value in $EDITOR
  get          Get the value of a key
  identity     Show or create the age encryption identity
  list         List the contents of all stores
  meta         View or modify metadata for a key
  move         Move a key
  remove       Delete one or more keys
  run          Get the value of a key and execute it
  set          Set a key to a given value

Store commands:
  export       Export store as NDJSON (alias for list --format ndjson)
  import       Restore key/value pairs from an NDJSON dump
  list-stores  List all stores
  move-store   Rename a store
  remove-store Delete a store

Git commands:
  git          Run any arbitrary command. Use with caution.
  init         Initialise pda! version control
  sync         Manually sync your stores with Git

Environment commands:
  config       View and modify configuration
  doctor       Check environment health

Additional Commands:
  completion   Generate the autocompletion script for the specified shell
  help         Help about any command
  version      Display pda! version
```

<p align="center"></p><!-- spacer -->

Most commands have aliases and flags. Run `pda help [command]` to see them.

### Key commands

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#setting"><code>pda set</code></a>,
    <a href="#getting"><code>pda get</code></a>,
    <a href="#running"><code>pda run</code></a>,
    <a href="#listing"><code>pda list</code></a>,
    <a href="#editing"><code>pda edit</code></a>,
    <a href="#moving--copying"><code>pda move</code></a>,
    <a href="#removing"><code>pda remove</code></a>
  </sup>
</p>

Use of `pda` revolves around creating keys with [`pda set`](#setting) and later retrieving them with [`pda get`](#getting). Keys can belong to a single store which can be set manually or left to default to the default store. Keys can be modified with [`pda edit`](#editing) and [`pda meta`](#metadata) for content or metadata editing respectively, and can be listed with [`pda list`](#listing). Keys are written as `KEY[@STORE]`. The default store can be configured with `store.default_store_name`.

Keys are capable of storing any arbitrary bytes and are not limited to just text.

Advanced usage of `pda` revolves around [templates](#templates) and [`pda run`](#running). 

#### Setting

[`pda set`](#setting) (alias: [`s`](#setting)) creates a key-value pair. Values can come from arguments, stdin, or a file.

```bash
Usage:
  pda set KEY[@STORE] [VALUE] [flags]

Aliases:
  set, s

Flags:
  -e, --encrypt        encrypt the value at rest using age
  -f, --file string    read value from a file
      --force          bypass read-only protection
  -h, --help           help for set
  -i, --interactive    prompt before overwriting an existing key
      --pin            pin the key (sorts to top in list)
      --readonly       mark the key as read-only
      --safe           do not overwrite if the key already exists
  -t, --ttl duration   expire the key after the provided duration (e.g. 24h, 30m)
```

[`pda set`](#setting) requires a key and a value as inputs. The first argument given will always be used to determine the key.

```bash
# create a key-value pair
pda set name "Alice"

# create a key-value pair with piped input
echo "Bob" | pda set name

# create a key-value pair with redirection
pda set example < silmarillion.txt

# create a pinned key-value pair from a file
pda set --pin example --file example.md

# create a key-value pair in the "Favourites" store
pda set movie@favourites "The Road"

# create an encrypted key-value pair, expiring in one day
pda set secret "Secret data." --encrypt --ttl 24h
```

The `interactive` and `safe` flags exist to prevent accidentally overwriting an existing key when creating a new one. These flags exist on all writable commands.

```bash
# prevent ever overwriting an existing key
pda set name "Bob" --safe

# guarantee a prompt when overwriting an existing key
pda set name "Joe" --interactive
```

Making a key `readonly` will also prevent unintended changes. It prevents making any changes unless `force` is passed or the key is made writable once again with [`pda edit`](#editing) or [`pda meta`](#metadata).

```bash
# create a readonly key-value pair
pda set repo "https://github.com/llywelwyn/pda" --readonly

# force-overwrite a readonly key-value pair
pda set dog "A four-legged mammal that isn't a cat." --force
```

#### Getting

[`pda get`](#getting) (alias: [`g`](#getting)) retrieves a key's value. [Templates](#templates) are evaluated at retrieval time.

```bash
Usage:
  pda get KEY[@STORE] [flags]

Aliases:
  get, g

Flags:
  -b, --base64        view binary data as base64
      --exists        exit 0 if the key exists, exit 1 if not (no output)
  -h, --help          help for get
      --no-template   directly output template syntax
  -c, --run           execute the result as a shell command
```

[`pda get`] takes one argument: the desired key. The value is output to stdout.

```bash
# get the value of a key
❯ pda get name
Alice
```

As mentioned in [setting](#setting), values support any arbitrary bytes. Values which are not valid UTF8 are retrieved slightly differently. Printing raw bytes directly in the terminal can (and will) cause [undefined behaviour](https://en.wikipedia.org/wiki/Undefined_behavior), so if a TTY is detected then a raw `pda get` will return instead some metadata about the contents of the bytes. In a non-TTY setting (when the data is piped or redirected), the raw bytes will be returned as expected.

If a representation of the bytes in a TTY is desired, the `base64` flag provides a safe way to view them.

```bash
# get the information of a non-UTF8 key
❯ pda get cat_gif
(size: 101.2k, image/gif)

# get the raw bytes of a non-UTF8 key via pipe
pda get cat_gif | xdg-open

# get the raw bytes of a non-UTF8 key via redirect
pda get cat_gif > cat.gif

# get the base64 representation of a non-UTF8 key
❯ pda get cat_gif --base64
R0lGODlhXANYAvf/MQAAAAEBAQICAgMDAwQEBAUFBQYGBggI...
```

The existence of a key can be checked with `exists`. It returns a `0 exit code` on an existent key, or a `1 exit code` on a non-existent one. This is primarily useful for scripting.

```bash
# check if an existent key exists
❯ pda get name --exists
exit code 0

# check if a non-existent key exists
❯ pda get nlammfd --exists
exit code 1
```

Running [`pda get`](#getting) will resolve templates in the stored key at run-time. This can be prevented with the `no-template` flag.

```bash
# set key "user" to a template of the USER environment variable
❯ pda set user "{{ env "USER" }}"

# get a templated key
❯ pda get user
lew

# get a templated key without resolving the template
❯ pda get user
{{ env "USER" }}
```

An alternative to [templates](#templates) is the `run` flag. For detailed information, see [`pda run`](#running), an alias for `pda get --run`.

```bash
# create a key containg a script
❯ pda set my_script "echo Hello, world."

# get and run a key using $SHELL
❯ pda get my_script --run
Hello, world.
```

#### Running

[`pda run`](#running) retrieves a key and executes it as a shell command. It uses the shell set in $SHELL. If, somehow, this environment variable is unset, it falls back and attempts to use `/bin/sh`. Templates are functional when running a key directly.

```bash
Usage:
  pda run KEY[@STORE] [flags]

Flags:
  -b, --base64        view binary data as base64
  -h, --help          help for run
      --no-template   directly output template syntax
```

Running takes one argument: the key.

```bash
# create a key containing a script, and a template
❯ pda set greet 'echo "Hello, {{ default "Jane Doe" .NAME }}"'

# run the key directly in $SHELL
❯ pda run greet
Hello, Jane Doe

# run the key, setting NAME to "Alice"
❯ pda run greet NAME="Alice"
Hello, Alice
```

#### Listing

[`pda list`](#listing) (alias: [`ls`](#listing)) shows what you've got stored. The default columns are `meta,size,ttl,store,key,value`. Meta is a 4-char flag string: `(e)ncrypted (w)ritable (t)tl (p)inned`, or a dash for an unset flag.

```bash
❯ pda ls
Meta Size TTL Store Key  Value
-w-p    5   - store todo don't forget this
----   23   - store url  https://prod.example.com
-w--    5   - store name Alice
```

By default, [`pda list`](#listing) shows entries from every store. Pass a store name to narrow to a single store:

```bash
pda ls @store
```

Use [`--store`](#filtering) / `-s` to filter stores by [glob pattern](#filtering):

```bash
pda ls --store "prod*"
```

Filter by key or value with [`--key`](#filtering) / `-k` and [`--value`](#filtering) / `-v`:

```bash
pda ls --key "db*" --value "**localhost**"
```

Columns can be toggled with `--no-X` flags. `--no-X` suppresses a column; `--no-X=false` adds it even if it's not in the default config:

```bash
# hide the meta and size columns
pda ls --no-meta --no-size
```

Long values are truncated to fit the terminal. [`--full`](#listing) / `-f` shows the complete value:

```bash
❯ pda ls
Key  Value
note this is a very long (..30 more chars)

❯ pda ls --full
Key  Value
note this is a very long value that keeps on going and going
```

[`--count`](#listing) / `-c` prints only the count of matching entries:

```bash
❯ pda ls --count
3

❯ pda ls --count --key "d*"
1
```

[`--format`](#listing) / `-o` selects the output format. Available formats: `table` (default), `csv`, `tsv`, `json`, `ndjson`, `markdown`, `html`:

```bash
❯ pda ls --format csv
Meta,Size,TTL,Store,Key,Value
-w--,5,-,store,name,Alice

❯ pda ls --format json
[{"key":"name","value":"Alice","encoding":"text","store":"store"}]
```

[`--all`](#listing) / `-a` lists across all stores (default when `list.always_show_all_stores` is true).

[`--base64`](#listing) / `-b` shows binary data as base64.

[`--no-header`](#listing) suppresses the header row.

[Pinned](#pinned) entries sort to the top, preserving alphabetical order within the pinned and unpinned groups.

<p>
  <sup>
    See also:
    <a href="#listing"><code>pda help list</code></a>
  </sup>
</p>

#### Editing

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#editing"><code>pda edit</code></a>,
    <a href="#setting"><code>pda set</code></a>,
    <a href="#metadata"><code>pda meta</code></a>
  </sup>
</p>

[`pda edit`](#editing) (alias: [`e`](#editing)) opens a key's value in your `$EDITOR`. If the key doesn't exist, an empty file is opened — saving non-empty content creates it.

```bash
# edit an existing key
pda edit name

# edit a new key — saving non-empty content creates it
pda edit newkey
```

Metadata flags can be passed alongside the edit to modify metadata in the same operation:

```bash
pda edit name --ttl 1h --encrypt
```

Trailing newlines added by the editor are stripped by default. [`--preserve-newline`](#editing) keeps them:

```bash
pda edit name --preserve-newline
```

[`--encrypt`](#editing) / `-e` encrypts the value. [`--decrypt`](#editing) / `-d` decrypts it. [`--readonly`](#editing) and [`--writable`](#editing) toggle protection. [`--pin`](#editing) and [`--unpin`](#editing) toggle pinning. [`--ttl`](#editing) sets or clears expiry (e.g. `30m`, `2h`, or `never`).

Binary values are presented as base64 for editing and decoded back on save.

[Read-only](#read-only) keys require [`--force`](#editing) to edit.

<p>
  <sup>
    See also:
    <a href="#editing"><code>pda help edit</code></a>
  </sup>
</p>

#### Moving & Copying

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#moving--copying"><code>pda move</code></a>,
    <a href="#moving--copying"><code>pda copy</code></a>
  </sup>
</p>

[`pda move`](#moving--copying) (alias: [`mv`](#moving--copying)) moves a key to a new name or store. All metadata is preserved.

```bash
❯ pda mv name name2
  ok renamed name to name2
```

[`pda copy`](#moving--copying) (alias: [`cp`](#moving--copying)) makes a copy. The source is kept and all metadata is preserved.

```bash
pda cp name name2
```

[`mv --copy`](#moving--copying) and [`cp`](#moving--copying) are equivalent:

```bash
pda mv name name2 --copy
```

Move or copy across stores:

```bash
pda mv name@store name@archive
pda cp config@dev config@prod
```

[`--safe`](#moving--copying) skips if the destination already exists:

```bash
pda mv name name2 --safe
# info skipped 'name2': already exists
```

[`--yes`](#moving--copying) / `-y` skips all confirmation prompts:

```bash
pda mv name name2 -y
```

[Read-only](#read-only) keys can't be moved or overwritten without [`--force`](#moving--copying):

```bash
❯ pda mv readonly-key newname
FAIL cannot move 'readonly-key': key is read-only

pda mv readonly-key newname --force
```

[`cp`](#moving--copying) can copy a read-only key freely (since the source isn't modified), and the copy preserves the read-only flag. Overwriting a read-only destination is blocked without [`--force`](#moving--copying).

<p>
  <sup>
    See also:
    <a href="#moving--copying"><code>pda help move</code></a>,
    <a href="#moving--copying"><code>pda help copy</code></a>
  </sup>
</p>

#### Removing

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#removing"><code>pda remove</code></a>,
    <a href="#filtering"><code>--key</code></a>
  </sup>
</p>

[`pda remove`](#removing) (alias: [`rm`](#removing)) deletes one or more keys.

```bash
pda rm kitty
```

Remove multiple keys at once:

```bash
pda rm kitty dog@animals
```

Mix exact keys with [glob patterns](#filtering) using [`--key`](#removing):

```bash
pda set cog "cogs"
pda set dog "doggy"
pda set kitty "cat"
pda rm kitty --key "?og"
```

Filter by store with [`--store`](#removing) / `-s` and by value with [`--value`](#removing) / `-v`:

```bash
pda rm --store "temp*" --key "session*"
```

[`--interactive`](#removing) / `-i` prompts before each deletion (or set `key.always_prompt_delete` in [config](#config)):

```bash
pda rm kitty -i
#  ??? remove 'kitty'? (y/n)
#  ==> y
```

Glob-matched deletions prompt by default (configurable with `key.always_prompt_glob_delete`).

[`--yes`](#removing) / `-y` auto-accepts all confirmation prompts:

```bash
pda rm kitty -y
```

[Read-only](#read-only) keys can't be deleted without [`--force`](#removing):

```bash
❯ pda rm protected-key
FAIL cannot remove 'protected-key': key is read-only

pda rm protected-key --force
```

<p>
  <sup>
    See also:
    <a href="#removing"><code>pda help remove</code></a>
  </sup>
</p>

### Metadata

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#metadata"><code>pda meta</code></a>,
    <a href="#ttl">TTL</a>,
    <a href="#encryption">Encryption</a>,
    <a href="#read-only">Read-Only</a>,
    <a href="#pinned">Pinned</a>
  </sup>
</p>

[`pda meta`](#metadata) views or modifies metadata for a key without changing its value. With no flags, it displays the key's current metadata:

```bash
❯ pda meta session
  key: session@store
  secret: false
  writable: true
  pinned: false
  expires: 59m30s
```

Pass flags to modify: [`--ttl`](#ttl), [`--encrypt`](#encryption) / [`--decrypt`](#encryption), [`--readonly`](#read-only) / [`--writable`](#read-only), [`--pin`](#pinned) / [`--unpin`](#pinned).

Multiple metadata changes can be combined in one call:

```bash
pda meta session --ttl 2h --encrypt --pin
```

Modifying a [read-only](#read-only) key's metadata requires [`--force`](#metadata) (except for toggling the read-only flag itself, and pin/unpin):

```bash
❯ pda meta api-url --ttl 1h
FAIL cannot meta 'api-url': key is read-only

pda meta api-url --ttl 1h --force
```

<p>
  <sup>
    See also:
    <a href="#metadata"><code>pda help meta</code></a>
  </sup>
</p>

#### TTL

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#setting"><code>pda set</code></a>,
    <a href="#metadata"><code>pda meta</code></a>
  </sup>
</p>

Keys can be given an expiration time. Expired keys are marked for garbage collection and deleted on the next access to the store.

Set a TTL at creation time with [`pda set --ttl`](#setting):

```bash
# expire after 1 hour
pda set session "123" --ttl 1h

# expire after 54 minutes and 10 seconds
pda set session2 "xyz" --ttl 54m10s
```

[`pda list`](#listing) shows expiration in the TTL column:

```bash
❯ pda ls
   TTL Key      Value
59m30s session  123
51m40s session2 xyz
```

Change or clear the TTL on an existing key with [`pda meta --ttl`](#metadata):

```bash
❯ pda meta session --ttl 2h
  ok set ttl to 2h session

❯ pda meta session --ttl never
  ok cleared ttl session
```

The [`edit`](#editing) command also accepts `--ttl`:

```bash
pda edit session --ttl 30m
```

[`export`](#import--export) and [`import`](#import--export) preserve the expiry date. Expirations are stored as a timestamp, not a timer — they continue ticking down regardless of whether the key is in an active store or sitting in a backup file.

<p>
  <sup>
    See also:
    <a href="#setting"><code>pda help set</code></a>,
    <a href="#metadata"><code>pda help meta</code></a>
  </sup>
</p>

#### Encryption

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#setting"><code>pda set</code></a>,
    <a href="#metadata"><code>pda meta</code></a>,
    <a href="#identity"><code>pda identity</code></a>
  </sup>
</p>

[`pda set --encrypt`](#setting) encrypts values at rest using [age](https://github.com/FiloSottile/age). Values are stored on disk as age ciphertext and decrypted automatically by commands like [`get`](#getting) and [`list`](#listing) when the correct identity file is present. An X25519 identity is generated on first use.

```bash
pda set --encrypt api-key "sk-live-abc123"
#   ok created identity at ~/.config/pda/identity.txt

pda set --encrypt token "ghp_xxxx"
```

[`pda get`](#getting) decrypts automatically:

```bash
❯ pda get api-key
sk-live-abc123
```

Toggle encryption on an existing key with [`pda meta`](#metadata):

```bash
❯ pda meta api-key --encrypt
  ok encrypted api-key

❯ pda meta api-key --decrypt
  ok decrypted api-key
```

The on-disk value is ciphertext, so encrypted entries are safe to commit and push with [Git](#git):

```bash
❯ pda export
{"key":"api-key","value":"YWdlLWVuY3J5cHRpb24u...","encoding":"secret"}
```

[`mv`](#moving--copying), [`cp`](#moving--copying), and [`import`](#import--export) all preserve encryption, read-only, and pinned flags. Overwriting an encrypted key without `--encrypt` will warn you:

```bash
pda cp api-key api-key-backup
# still encrypted

❯ pda set api-key "oops"
WARN overwriting encrypted key 'api-key' as plaintext
hint pass --encrypt to keep it encrypted
```

If the identity file is missing, encrypted values are inaccessible but not lost. Keys remain visible, and the ciphertext is preserved through reads and writes:

```bash
❯ pda ls
Meta Key     Value
ew-- api-key locked (identity file missing)

❯ pda get api-key
FAIL cannot get 'api-key': secret is locked (identity file missing)
```

All encryption operations can be set as default with `key.always_encrypt` in [config](#config), so every [`pda set`](#setting) automatically encrypts.

<p>
  <sup>
    See also:
    <a href="#setting"><code>pda help set</code></a>,
    <a href="#metadata"><code>pda help meta</code></a>,
    <a href="#identity"><code>pda help identity</code></a>
  </sup>
</p>

#### Read-Only

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#setting"><code>pda set</code></a>,
    <a href="#metadata"><code>pda meta</code></a>,
    <a href="#editing"><code>pda edit</code></a>
  </sup>
</p>

Keys marked read-only are protected from accidental modification. You can modify a read-only key again by making it [`--writable`](#metadata) or by explicitly bypassing with [`--force`](#metadata).

Set a key as read-only at creation time:

```bash
pda set api-url "https://prod.example.com" --readonly
```

Toggle with [`pda meta`](#metadata):

```bash
❯ pda meta api-url --readonly
  ok made readonly api-url

❯ pda meta api-url --writable
  ok made writable api-url
```

Or alongside an edit:

```bash
pda edit notes --readonly
```

Read-only keys are protected from [`set`](#setting), [`rm`](#removing), [`mv`](#moving--copying), and [`edit`](#editing). Use `--force` to bypass:

```bash
❯ pda set api-url "new value"
FAIL cannot set 'api-url': key is read-only

pda set api-url "new value" --force
pda rm api-url --force
pda mv api-url new-name --force
```

Modifying a read-only key's metadata also requires `--force` (except for toggling the read-only flag itself, and pin/unpin):

```bash
❯ pda meta api-url --ttl 1h
FAIL cannot meta 'api-url': key is read-only

pda meta api-url --ttl 1h --force
```

[`cp`](#moving--copying) can copy a read-only key freely (since the source isn't modified), and the copy preserves the read-only flag. Overwriting a read-only destination is blocked without `--force`.

<p>
  <sup>
    See also:
    <a href="#setting"><code>pda help set</code></a>,
    <a href="#metadata"><code>pda help meta</code></a>,
    <a href="#editing"><code>pda help edit</code></a>
  </sup>
</p>

#### Pinned

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#setting"><code>pda set</code></a>,
    <a href="#metadata"><code>pda meta</code></a>,
    <a href="#listing"><code>pda list</code></a>
  </sup>
</p>

Pinned keys sort to the top of [`pda list`](#listing) output, preserving alphabetical order within the pinned and unpinned groups.

Pin a key at creation time:

```bash
pda set important "remember this" --pin
```

Toggle with [`pda meta`](#metadata):

```bash
❯ pda meta todo --pin
  ok pinned todo

❯ pda meta todo --unpin
  ok unpinned todo
```

```bash
❯ pda ls
Meta Key       Value
-w-p important remember this
-w-- name      Alice
-w-- other     foo
```

<p>
  <sup>
    See also:
    <a href="#setting"><code>pda help set</code></a>,
    <a href="#metadata"><code>pda help meta</code></a>
  </sup>
</p>

### Stores

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#stores"><code>pda list-stores</code></a>,
    <a href="#stores"><code>pda move-store</code></a>,
    <a href="#stores"><code>pda remove-store</code></a>
  </sup>
</p>

You can have as many stores as you want. Stores are created implicitly when you set a key with a `@STORE` suffix. Each store is a separate NDJSON file on disk.

[`pda list-stores`](#stores) (alias: [`lss`](#stores)) shows all stores with key counts and file sizes:

```bash
❯ pda list-stores
Keys  Size Store
   2 1.8k @birthdays
  12 4.2k @store
```

[`--short`](#stores) prints only the names:

```bash
❯ pda list-stores --short
@birthdays
@store
```

Save to a specific store with the `@STORE` syntax:

```bash
pda set alice@birthdays "11/11/1998"
```

List a specific store:

```bash
❯ pda ls @birthdays
   Store Key   Value
birthdays alice 11/11/1998
birthdays bob   05/12/1980
```

[`pda move-store`](#stores) (alias: [`mvs`](#stores)) renames a store:

```bash
pda move-store birthdays bdays
```

Copy a store with `--copy`:

```bash
pda move-store birthdays bdays --copy
```

[`--safe`](#stores) skips if the destination already exists:

```bash
pda move-store birthdays bdays --safe
```

[`pda remove-store`](#stores) (alias: [`rms`](#stores)) deletes a store:

```bash
pda remove-store birthdays
```

[`--yes`](#stores) / `-y` skips confirmation prompts:

```bash
pda remove-store birthdays -y
```

<p>
  <sup>
    See also:
    <a href="#stores"><code>pda help list-stores</code></a>,
    <a href="#stores"><code>pda help move-store</code></a>,
    <a href="#stores"><code>pda help remove-store</code></a>
  </sup>
</p>

#### Import & Export

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#import--export"><code>pda export</code></a>,
    <a href="#import--export"><code>pda import</code></a>
  </sup>
</p>

[`pda export`](#import--export) exports everything as NDJSON (it's an alias for `list --format ndjson`):

```bash
pda export > my_backup
```

Filter exports with [`--key`](#filtering), [`--value`](#filtering), and [`--store`](#filtering):

```bash
# export only matching keys
pda export --key "a*"

# export only entries whose values contain a URL
pda export --value "**https**"
```

[`pda import`](#import--export) restores entries from an NDJSON dump. By default, each entry is routed to the store it came from (via the `"store"` field in the NDJSON). If no `"store"` field is present, entries go to `store.default_store_name`.

```bash
# entries are routed to their original stores
pda import -f my_backup
#   ok restored 5 entries
```

Pass a store name as a positional argument to force all entries into one store:

```bash
pda import mystore -f my_backup
#   ok restored 5 entries into @mystore
```

Read from stdin:

```bash
pda import < my_backup
```

Filter imports with [`--key`](#filtering) and [`--store`](#filtering):

```bash
# import only matching keys
pda import --key "a*" -f my_backup

# import only entries from matching stores
pda import --store "prod*" -f my_backup
```

[`--drop`](#import--export) does a full replace — drops all existing entries before importing:

```bash
pda import --drop -f my_backup
```

[`--interactive`](#import--export) / `-i` prompts before overwriting existing keys.

[`export`](#import--export) encodes [binary data](#binary-data) as base64. [Encryption](#encryption), [read-only](#read-only), [pinned](#pinned) flags, and [TTL](#ttl) are all preserved through export and import.

<p>
  <sup>
    See also:
    <a href="#import--export"><code>pda help export</code></a>,
    <a href="#import--export"><code>pda help import</code></a>
  </sup>
</p>

### Templates

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#getting"><code>pda get</code></a>,
    <a href="#running"><code>pda run</code></a>
  </sup>
</p>

Values support Go's [`text/template`](https://pkg.go.dev/text/template) syntax. Templates are evaluated on [`pda get`](#getting) and [`pda run`](#running).

`text/template` is a Turing-complete templating library that supports pipelines, nested templates, conditionals, loops, and more. Actions are given with `{{ action }}` syntax. To fit `text/template` into a CLI key-value tool, `pda!` adds a small set of built-in functions on top of the standard library.

These same functions are also available in `git.default_commit_message` templates, along with `summary` which returns the action that triggered the commit (e.g. "set foo", "removed bar").

#### Basic Substitution

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>,
    <a href="#getting"><code>pda get</code></a>
  </sup>
</p>

Template variables are substituted from `KEY=VALUE` arguments passed to [`pda get`](#getting):

```bash
pda set greeting "Hello, {{ .NAME }}"

❯ pda get greeting NAME="Alice"
Hello, Alice
```

#### `default`

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>
  </sup>
</p>

`default` sets a fallback value when a variable is missing or empty:

```bash
pda set greeting "Hello, {{ default "World" .NAME }}"

❯ pda get greeting
Hello, World

❯ pda get greeting NAME="Bob"
Hello, Bob
```

#### `require`

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>
  </sup>
</p>

`require` errors if the variable is missing or empty:

```bash
pda set file "{{ require .FILE }}"

❯ pda get file
FAIL cannot get 'file': ...required value is missing or empty
```

#### `env`

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>
  </sup>
</p>

`env` reads from environment variables:

```bash
pda set my_name "{{ env "USER" }}"

❯ pda get my_name
llywelwyn
```

#### `time`

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>
  </sup>
</p>

`time` returns the current UTC time in RFC3339 format:

```bash
pda set note "Created at {{ time }}"

❯ pda get note
Created at 2025-01-15T12:00:00Z
```

#### `enum`

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>
  </sup>
</p>

`enum` restricts a variable to a set of acceptable values:

```bash
pda set level "Log level: {{ enum .LEVEL "info" "warn" "error" }}"

❯ pda get level LEVEL=info
Log level: info

❯ pda get level LEVEL=debug
FAIL cannot get 'level': ...invalid value 'debug', allowed: [info warn error]
```

#### `int`

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>
  </sup>
</p>

`int` parses a variable as an integer, useful for loops and arithmetic:

```bash
pda set number "{{ int .N }}"

❯ pda get number N=3
3
```

Use it in a range loop:

```bash
pda set meows "{{ range int .COUNT }}meow! {{ end }}"

❯ pda get meows COUNT=4
meow! meow! meow! meow!
```

#### `list`

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>
  </sup>
</p>

`list` parses a comma-separated string into a list for iteration:

```bash
pda set names "{{ range list .NAMES }}Hi {{.}}. {{ end }}"

❯ pda get names NAMES=Bob,Alice
Hi Bob. Hi Alice.
```

#### `shell`

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>
  </sup>
</p>

`shell` executes a command and returns its stdout:

```bash
pda set rev '{{ shell "git rev-parse --short HEAD" }}'

❯ pda get rev
a1b2c3d
```

```bash
pda set today '{{ shell "date +%Y-%m-%d" }}'

❯ pda get today
2025-06-15
```

#### `pda` (Recursive)

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>
  </sup>
</p>

`pda` gets another key's value, enabling recursive composition:

```bash
pda set base_url "https://api.example.com"
pda set endpoint '{{ pda "base_url" }}/users/{{ require .ID }}'

❯ pda get endpoint ID=42
https://api.example.com/users/42
```

Cross-store references work too:

```bash
pda set host@urls "https://example.com"
pda set api '{{ pda "host@urls" }}/api'

❯ pda get api
https://example.com/api
```

#### `no-template`

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#getting"><code>pda get</code></a>
  </sup>
</p>

Pass [`--no-template`](#getting) to [`pda get`](#getting) to output the raw value without evaluating templates:

```bash
pda set hello "{{ if .MORNING }}Good morning.{{ end }}"

❯ pda get hello MORNING=1
Good morning.

❯ pda get hello --no-template
{{ if .MORNING }}Good morning.{{ end }}
```

<p>
  <sup>
    See also:
    <a href="#getting"><code>pda help get</code></a>,
    <a href="#setting"><code>pda help set</code></a>
  </sup>
</p>

### Filtering

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#listing"><code>pda list</code></a>,
    <a href="#removing"><code>pda remove</code></a>,
    <a href="#import--export"><code>pda export</code></a>,
    <a href="#import--export"><code>pda import</code></a>
  </sup>
</p>

[`--key`](#filtering) / `-k`, [`--value`](#filtering) / `-v`, and [`--store`](#filtering) / `-s` filter entries with glob support. All three flags are repeatable, with results matching one-or-more of the patterns per flag. When multiple flags are combined, results must satisfy all of them (AND across flags, OR within the same flag).

These filters work with [`list`](#listing), [`export`](#import--export), [`import`](#import--export), and [`remove`](#removing). [`--value`](#filtering) is not available on [`import`](#import--export) or [`remove`](#removing).

[`gobwas/glob`](https://github.com/gobwas/glob) is used for matching. The default separators are `/-_.@:` and space.

#### Glob Patterns

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#filtering">Filtering</a>
  </sup>
</p>

`*` wildcards a word or series of characters, stopping at separator boundaries:

```bash
❯ pda ls
cat
dog
cog
mouse hotdog
mouse house
foo.bar.baz

pda ls --key "*"
# cat, dog, cog (single-segment keys only)

pda ls --key "* *"
# mouse hotdog, mouse house

pda ls --key "foo.*.baz"
# foo.bar.baz
```

`**` super-wildcards ignore word boundaries:

```bash
pda ls --key "foo**"
# foo.bar.baz

pda ls --key "**g"
# dog, cog, mouse hotdog
```

`?` matches a single character:

```bash
pda ls --key "?og"
# dog, cog
```

`[abc]` matches one of the characters in the brackets:

```bash
pda ls --key "[dc]og"
# dog, cog

# negate with '!'
pda ls --key "[!dc]og"
# bog (if it exists)
```

`[a-c]` matches a range:

```bash
pda ls --key "[a-g]ag"
# bag, gag

pda ls --key "[!a-g]ag"
# wag
```

#### Filtering by Key

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#filtering">Filtering</a>,
    <a href="#listing"><code>pda list</code></a>
  </sup>
</p>

[`--key`](#filtering) / `-k` filters entries by key name:

```bash
pda ls --key "db*"
pda ls --key "session*" --key "token*"
```

Multiple `--key` patterns are OR'd — an entry matches if it matches any of them.

#### Filtering by Value

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#filtering">Filtering</a>,
    <a href="#listing"><code>pda list</code></a>
  </sup>
</p>

[`--value`](#filtering) / `-v` filters by value content using the same glob syntax:

```bash
❯ pda ls --value "**localhost**"
Key    Value
db-url postgres://localhost:5432
```

Multiple `--value` patterns are OR'd:

```bash
❯ pda ls --value "**world**" --value "42"
Key      Value
greeting hello world
number   42
```

Locked (encrypted without an available identity) and non-UTF-8 (binary) entries are silently excluded from `--value` matching.

#### Filtering by Store

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#filtering">Filtering</a>,
    <a href="#listing"><code>pda list</code></a>
  </sup>
</p>

[`--store`](#filtering) / `-s` filters by store name:

```bash
pda ls --store "prod*"
pda export --store "dev*"
```

#### Combining Filters

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#filtering">Filtering</a>
  </sup>
</p>

Combine key, value, and store filters. Results must match all flags (AND), with OR within each flag:

```bash
pda ls --key "db*" --value "**localhost**"
```

Globs can be arbitrarily complex, and [`--key`](#filtering) can be combined with exact positional args on [`rm`](#removing):

```bash
pda rm cat --key "{mouse,[cd]og}**"
#  ??? remove 'cat'? (y/n)
#  ==> y
#  ??? remove 'mouse trap'? (y/n)
# ...
```

<p>
  <sup>
    See also:
    <a href="#listing"><code>pda help list</code></a>,
    <a href="#removing"><code>pda help remove</code></a>
  </sup>
</p>

### Binary Data

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#setting"><code>pda set</code></a>,
    <a href="#getting"><code>pda get</code></a>,
    <a href="#listing"><code>pda list</code></a>
  </sup>
</p>

`pda!` supports all binary data. Save it with [`pda set`](#setting):

```bash
pda set logo < logo.png
pda set logo -f logo.png
```

And retrieve it with [`pda get`](#getting):

```bash
pda get logo > output.png
```

On a TTY, [`get`](#getting) and [`list`](#listing) show a summary for binary data. If piped or run outside of a TTY, raw bytes are output:

```bash
❯ pda get logo
(binary: 4.2 KB, image/png)
```

[`--base64`](#getting) / `-b` views binary data as base64:

```bash
❯ pda get logo --base64
iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADklEQVQI12...
```

[`pda export`](#import--export) encodes binary data as base64 in the NDJSON:

```bash
❯ pda export
{"key":"logo","value":"89504E470D0A1A0A0000000D4948445200000001000000010802000000","encoding":"base64"}
```

[`pda edit`](#editing) presents binary values as base64 for editing and decodes them back on save.

<p>
  <sup>
    See also:
    <a href="#setting"><code>pda help set</code></a>,
    <a href="#getting"><code>pda help get</code></a>
  </sup>
</p>

### Git

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#git"><code>pda init</code></a>,
    <a href="#sync"><code>pda sync</code></a>,
    <a href="#config">Config</a>
  </sup>
</p>

`pda!` supports automatic version control backed by Git, either in a local-only repository or by initialising from a remote.

#### Init

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#git">Git</a>,
    <a href="#sync"><code>pda sync</code></a>
  </sup>
</p>

[`pda init`](#git) initialises version control:

```bash
# initialise an empty repository
pda init

# or clone an existing one
pda init https://github.com/llywelwyn/my-repository
```

[`--clean`](#git) removes the existing `.git` directory first, useful for reinitialising or switching remotes:

```bash
pda init --clean
pda init https://github.com/llywelwyn/my-repository --clean
```

<p>
  <sup>
    See also:
    <a href="#git"><code>pda help init</code></a>
  </sup>
</p>

#### Sync

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#git">Git</a>
  </sup>
</p>

[`pda sync`](#sync) conducts a best-effort sync of your local data with your Git repository. Any time you swap machine or know you've made changes outside of `pda!`, syncing is recommended.

If you're ahead, syncing will commit and push. If you're behind, syncing will detect this and prompt you: either stash local changes and pull, or abort and fix manually.

```bash
# sync with Git
pda sync

# with a custom commit message
pda sync -m "added production credentials"
```

Running [`pda sync`](#sync) manually will always fetch, commit, and push — or stash and pull if behind — regardless of config.

<p>
  <sup>
    See also:
    <a href="#sync"><code>pda help sync</code></a>
  </sup>
</p>

#### Auto-Commit & Auto-Push

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#git">Git</a>,
    <a href="#config">Config</a>
  </sup>
</p>

`pda!` supports automation via its [config](#config). There are options for `git.auto_commit`, `git.auto_fetch`, and `git.auto_push`.

**`git.auto_commit`** commits changes immediately to the local Git repository any time data is changed.

**`git.auto_fetch`** fetches before committing any changes. This incurs a noticeable slowdown due to network round-trips.

**`git.auto_push`** automatically pushes committed changes to the remote repository, if one is configured.

If `auto_commit` is false, `auto_fetch` and `auto_push` have no effect. They are additional steps in the commit process.

A recommended setup is to enable `git.auto_commit` and run [`pda sync`](#sync) manually when switching machines.

### Identity

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#identity"><code>pda identity</code></a>,
    <a href="#encryption">Encryption</a>
  </sup>
</p>

[`pda identity`](#identity) (alias: [`id`](#identity)) manages the age encryption identity used for [encryption](#encryption).

#### Viewing Identity

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#identity"><code>pda identity</code></a>
  </sup>
</p>

With no flags, [`pda identity`](#identity) shows your public key, identity file path, and any additional recipients:

```bash
❯ pda identity
  ok pubkey  age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p
  ok identity  ~/.config/pda/identity.txt
```

[`--path`](#identity) prints only the identity file path:

```bash
❯ pda identity --path
~/.config/pda/identity.txt
```

#### Creating an Identity

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#identity"><code>pda identity</code></a>
  </sup>
</p>

An identity is generated automatically the first time you use [`--encrypt`](#encryption). To create one manually:

```bash
pda identity --new
```

[`--new`](#identity) errors if an identity already exists. Delete the file manually to replace it.

#### Recipients

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#identity"><code>pda identity</code></a>,
    <a href="#encryption">Encryption</a>
  </sup>
</p>

By default, secrets are encrypted only for your own identity. To encrypt for additional recipients (e.g. a teammate or another device), use [`--add-recipient`](#identity) with their age public key. All existing secrets are automatically re-encrypted for every recipient:

```bash
❯ pda identity --add-recipient age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p
  ok re-encrypted api-key
  ok added recipient age1ql3z...
  ok re-encrypted 1 secret(s)
```

Remove a recipient with [`--remove-recipient`](#identity). Secrets are re-encrypted without their key:

```bash
pda identity --remove-recipient age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p
```

Additional recipients are shown in the default identity display:

```bash
❯ pda identity
  ok pubkey     age1abc...
  ok identity   ~/.local/share/pda/identity.txt
  ok recipient  age1ql3z...
```

<p>
  <sup>
    See also:
    <a href="#identity"><code>pda help identity</code></a>
  </sup>
</p>

### Config

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#config"><code>pda config</code></a>,
    <a href="#doctor"><code>pda doctor</code></a>
  </sup>
</p>

Config is stored at `~/.config/pda/config.toml` (Linux/macOS) or `%LOCALAPPDATA%/pda/config.toml` (Windows). All values have sensible defaults, so a config file is entirely optional.

#### Config Commands

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#config"><code>pda config</code></a>
  </sup>
</p>

[`pda config`](#config) manages configuration without editing files by hand:

```bash
# list all config values and their current settings
pda config list

# get a single value
❯ pda config get git.auto_commit
false

# set a value (validated before saving)
pda config set git.auto_commit true

# open in $EDITOR (validated on save)
pda config edit

# print the config file path
pda config path

# generate a fresh default config file
pda config init

# overwrite an existing config with defaults
pda config init --new

# update config: migrate deprecated keys and fill missing defaults
pda config init --update
```

[`pda doctor`](#doctor) will warn about unrecognised keys (typos, removed options) and show any non-default values, so it doubles as a config audit.

<p>
  <sup>
    See also:
    <a href="#config"><code>pda help config</code></a>
  </sup>
</p>

#### Example config.toml

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#config">Config</a>
  </sup>
</p>

All values below are the defaults. A missing config file or missing keys will use these values.

```toml
# display ascii header in long root and version commands
display_ascii_art = true

[key]
# prompt y/n before deleting keys
always_prompt_delete = false
# prompt y/n before deleting with a glob match
always_prompt_glob_delete = true
# prompt y/n before key overwrites
always_prompt_overwrite = false
# encrypt all values at rest by default
always_encrypt = false

[store]
# store name used when none is specified
default_store_name = "store"
# prompt y/n before deleting whole store
always_prompt_delete = true
# prompt y/n before store overwrites
always_prompt_overwrite = true

[list]
# list all, or list only the default store when none specified
always_show_all_stores = true
# default output, accepts: table|tsv|csv|markdown|html|ndjson|json
default_list_format = "table"
# show full values without truncation
always_show_full_values = false
# suppress the header row
always_hide_header = false
# columns and order, accepts: meta,size,ttl,store,key,value
default_columns = "meta,size,ttl,store,key,value"

[git]
# auto fetch whenever a change happens
auto_fetch = false
# auto commit any changes
auto_commit = false
# auto push after committing
auto_push = false
# commit message if none manually specified
# supports templates, see: #templates section
default_commit_message = "{{ summary }} {{ time }}"
```

### Environment

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#config">Config</a>,
    <a href="#doctor"><code>pda doctor</code></a>
  </sup>
</p>

`PDA_CONFIG` overrides the config directory. `pda!` will look for `config.toml` in this directory:

```bash
PDA_CONFIG=/tmp/config/ pda set key value
```

`PDA_DATA` overrides the data storage directory:

```bash
PDA_DATA=/tmp/stores pda set key value
```

Default data locations:
- Linux: `~/.local/share/pda/`
- macOS: `~/Library/Application Support/pda/`
- Windows: `%LOCALAPPDATA%/pda/`

`EDITOR` is used by [`pda edit`](#editing) and [`pda config edit`](#config) to open values in a text editor. Must be set for these commands to work:

```bash
EDITOR=nvim pda edit mykey
```

`SHELL` is used by [`pda run`](#running) (or [`pda get --run`](#getting)) for command execution. Falls back to `/bin/sh` if unset:

```bash
pda run script
```

### Doctor

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#config">Config</a>,
    <a href="#environment">Environment</a>
  </sup>
</p>

[`pda doctor`](#doctor) runs a set of health checks of your environment:

```bash
❯ pda doctor
  ok pda! 2025.52 Christmas release (linux/amd64)
  ok OS: Linux 6.18.7-arch1-1
  ok Go: go1.23.0
  ok Git: 2.45.0
  ok Shell: /bin/zsh
  ok Config: /home/user/.config/pda
  ok Non-default config:
     ├── display_ascii_art: false
     └── git.auto_commit: true
  ok Data: /home/user/.local/share/pda
  ok Identity: /home/user/.config/pda/identity.txt
  ok Git initialised on main
  ok Git remote configured
  ok Git in sync with remote
  ok 3 store(s), 15 key(s), 2 secret(s), 4.2k total size
  ok No issues found
```

Severity levels are colour-coded: `ok` (green), `WARN` (yellow), and `FAIL` (red). Only `FAIL` produces a non-zero exit code. `WARN` is generally not a problem, but may mean some functionality isn't being made use of, like version control not having been initialised yet.

<p>
  <sup>
    See also:
    <a href="#doctor"><code>pda help doctor</code></a>
  </sup>
</p>

### Help & Version

<p>
  <sup>
    <a href="#overview">↑</a>
  </sup>
</p>

```bash
# help for any command
pda help set
pda help list
pda help config

# display the full version output
pda version

# or just the release
❯ pda version --short
pda! 2025.52 Christmas release
```

`pda!` uses calendar versioning: `YYYY.WW`. ASCII art can be permanently disabled with `display_ascii_art = false` in [config](#config).

### License

<p>
  <sup>
    <a href="#overview">↑</a>
  </sup>
</p>

MIT — see [LICENSE](LICENSE).
