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

`pda` stores key-value pairs natively as [newline-delimited JSON](https://en.wikipedia.org/wiki/JSON_streaming#Newline-delimited_JSON) files. [`pda list`](#listing) outputs tabular data by default, but also supports [CSV](https://en.wikipedia.org/wiki/Comma-separated_values), [TSV](https://en.wikipedia.org/wiki/Tab-separated_values), [Markdown](https://en.wikipedia.org/wiki/Markdown) and [HTML](https://en.wikipedia.org/wiki/HTML) tables, [JSON](https://en.wikipedia.org/wiki/JSON), and raw NDJSON. Everything is in plaintext to make version control easy, and to avoid tying anybody to using this tool forever.

Git versioning can be initiated with [`pda init`](#git), and varying levels of automation can be toggled via the [config](#config): `git.autocommit`, `git.autofetch`, and `git.autopush`. Running Git operations on every change can be slow, but a commit is fast. A happy middle-ground is enabling `git.autocommit` and doing the rest manually via [`pda sync`](#git) when changing devices.

[Templates](#templates) are a primary feature, enabling values to make use of substitutions, environment variables, arbitrary shell execution, cross-references to other keys, and more.

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
  <a href="#binary-data">Binary&nbsp;Data</a>&nbsp;·
  <a href="#git">Git</a>&nbsp;·
  <a href="#identity">Identity</a>&nbsp;·
  <a href="#config">Config</a>&nbsp;·
  <a href="#environment">Environment</a>&nbsp;·
  <a href="#doctor">Doctor</a>&nbsp;·
  <a href="#version">Version</a>&nbsp;·
  <a href="#help">Help</a>
</div>

<p align="center"></p><!-- spacer -->

Most commands have aliases and flags. Run `pda help [command]` to see them.

### Key commands

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#setting"><code>Setting</code></a>,
    <a href="#getting"><code>Getting</code></a>,
    <a href="#running"><code>Running</code></a>,
    <a href="#listing"><code>Listing</code></a>,
    <a href="#editing"><code>Editing</code></a>,
    <a href="#moving--copying"><code>Moving and Copying</code></a>,
    <a href="#removing"><code>Removing</code></a>
  </sup>
</p>

Use of `pda` revolves around creating keys with [`pda set`](#setting) and later retrieving them with [`pda get`](#getting). Keys can belong to a single store which can be set manually or left to default to the default store. Keys can be modified with [`pda edit`](#editing) and [`pda meta`](#metadata) for content or metadata editing respectively, and can be listed with [`pda list`](#listing). Keys are written as `KEY[@STORE]`. The default store can be configured with `store.default_store_name`.

Keys are capable of storing any arbitrary bytes and are not limited to just text.

Advanced usage of `pda` revolves around [templates](#templates) and [`pda run`](#running). 

#### Setting

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-set"><code>pda set</code></a>
  </sup>
</p>

[`pda set`](#setting) (alias: [`s`](#setting)) creates a key-value pair, and requires a key and a value as inputs. The first argument given will always be used to determine the key (and store). Values can come from arguments, stdin, or a file.

```bash
# create a key-value pair
pda set name "Alice"

# create a key-value pair in the "Favourites" store
pda set movie@favourites "The Road"

# create a key-value pair with piped input
echo "Bob" | pda set name

# create a key-value pair with redirection
pda set example < silmarillion.txt

# create a pinned key-value pair from a file
pda set --pin example --file example.md

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

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a> ·
    <a href="#pda-get"><code>pda get</code></a>
  </sup>
</p>

[`pda get`](#getting) (alias: [`g`](#getting)) can be used to retrieve a key's value, and takes one argument: the desired key. The value is output to stdout. [Templates](#templates) are evaluated at retrieval time unless opted-out via the `no-template` flag.

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
❯ pda get user --no-template
{{ env "USER" }}
```

#### Running

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-get">pda get --run</a> ·
    <a href="#pda-run"><code>pda run</code></a>
  </sup>
</p>

[`pda run`](#running) retrieves a key and executes it as a shell command. It uses the shell set in `$SHELL. If, somehow, this environment variable is unset, it falls back and attempts to use `/bin/sh`. 

Running takes one argument: the key. [`pda run`](#running) and [`pda get --run`](#getting) are functionally equivalent.

```bash
# create a key containg a script
❯ pda set my_script "echo Hello, world."

# get and run a key using $SHELL
❯ pda get my_script --run
Hello, world.

❯ pda run my_script
Hello, world.
```

[Templates](#templates) are fully resolved before any shell execution happens.

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

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-list"><code>pda list</code></a> ·
    <a href="#filtering"><code>Filtering</code></a>
  </sup>
</p>

[`pda list`](#listing) (alias: [`ls`](#listing)) displays your key-value pairs. The default columns are `meta,size,ttl,store,key,value`. Meta is a 4-char flag string: `(e)ncrypted (w)ritable (t)tl (p)inned`, or a dash for an unset flag.

  -b, --base64          view binary data as base64
  -c, --count           print only the count of matching entries

[`pda list`](#listing) (alias: [`ls`](#listing)) displays stored key-value pairs with [pinned](#pinning) keys first, followed by alphabetical order. Default behaviour is to list [metadata](#metadata), size, [time-to-live](#ttl), store, the key, and value as a table. The order and visibility of every column can be toggled either via `list.default_columns` in the [config](#config) or via one-off flags.

It accepts one or zero arguments. If no argument is passed, with a default configuration [`pda list`](#listing) will display all keys from all stores, but this behaviour can be toggled to instead display only keys from the default store. If a store name is passed as an argument, only the keys contained within that store will be listed.

If `list.always_show_all_stores` is toggled off, the behaviour can be enabled on an individual basis by passing `all`.

```bash
# list all store contents
❯ pda list
Meta Size TTL Store Key  Value
-w-p    5   - todos todo don't forget this
----   23   - store url  https://example.com
-w--    5   -    me name Alice

# list only the contents of the "todos" store
❯ pda list todos
Meta Size TTL Store Key  Value
-w-p    5   - todos todo don't forget this

# list all store contents, but without Meta, Size, or TTL
❯ pda list --no-ttl --no-header --no-size
Store Key  Value
todos todo don't forget this
store url  https://example.com
   me name Alice

# count the number of entries for a given query
❯ pda list --count
3
```

When [listing](#listing), [glob patterns](#filtering) can be used to further filter by `key`, `store`, or `value`.

```bash
# list all store contents beginning with "https"
pda ls --value "https**"

# list all store contents beginning with "https" with "db" in the key
pda ls --value "https**" --key "**db**"
```

The standard tabular output of [`pda list`](#listing) can be swapped out for tab-separated values, comma-separated values, a markdown table, a HTML table, newline-delimited JSON, or JSON. Newline-delimited JSON is the native storage format of a `pda` store.

```bash
# list all store contents as comma-separated values
❯ pda ls --format csv
Meta,Size,TTL,Store,Key,Value
-w--,5,-,store,name,Alice

# list all store contents as JSON
❯ pda ls --format json
[{"key":"name","value":"Alice","encoding":"text","store":"store"}]
```

By default, long values are truncated to fit the terminal, but this behaviour can be toggled via the [config](#config) or by passing `full`. The setting is `list.always_show_full_values`.

```bash
❯ pda ls
Key  Value
note this is a very long (..30 more chars)

❯ pda ls --full
Key  Value
note this is a very long value that keeps on going and going
```

As with [`getting`](#getting), non-UTF8 data in lists will be substituted for a summary rather than displaying raw bytes. This can be changed out for a base64 representation by passing `base64`.

#### Editing

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-edit"><code>pda edit</code></a>
  </sup>
</p>

[`pda edit`](#editing) (alias: [`e`](#editing)) opens a key's value in your `$EDITOR`. If the key doesn't exist, an empty file is opened, and saving non-empty content creates it.

When [editing](#editing) a key, [metadata](#metadata)-altering flags can be passed in the same operation. These alterations will only take place if the edit is finalised by saving.

```bash
# edit an existing key or create a new one
pda edit name

# edit a key and pin it
pda edit name --pin

# edit a key and give it an expiration, and encrypt it
pda edit secret_stuff --ttl 1h --encrypt

# edit a key and make it readonly
pda edit do_not_change --readonly
```

Most `$EDITOR` will add a trailing newline on saving a file. These are stripped by default, but this behaviour can be toggled via `edit.always_preserve_newline` in the [config](#config) or as a one-off by passing `preserve-newline`.

```bash
# edit a key and preserve the automatic $EDITOR newline
pda edit example --preserve-newline
```

Binary values will be presented as base64 for editing and will be decoded back to raw bytes on save. [Read-only](#read-only) keys require being made writable before they can be edited, or `force` can be explicitly passed.

#### Moving & Copying

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-move"><code>pda move</code></a>,
    <a href="#pda-copy"><code>pda copy</code></a>
  </sup>
</p>

To move (or rename) a key, [`pda move`](#moving--copying) (alias: [`mv`](#moving--copying)) can be used. To copy a key, [`pda move --copy`](#moving--copying) (alias: [`cp`](#moving--copying)) can be used. With both of these operations, all [metadata](#metadata) is preserved.

```bash
# rename a key
❯ pda move name name2
  ok renamed name to name2

# move a key across stores
❯ pda mv name name@some_other_store
  ok renamed name to name@some_other_store

# copy a key
❯ pda cp name name2
  ok copied name to name2
```

Accidental overwrites have a few ways of being prevented. `safe` exists for moving and copying as it does for all changeful commands, skipping an operation if a destination already exists that would be overwritten. A [read-only](#read-only) key will also prevent being moved or overwritten unless `force` is explicitly passed.

Additionally, `interactive` being passed or `key.always_prompt_overwrite` being enabled in the [config](#config) will cause a yes-no prompt to be presented if a key is going to be overwritten. Inversely, prompts can always be skipped by passing `yes`.

```bash
# move a key safely
❯ pda mv name name2 --safe
# info skipped 'name2': already exists

# move a key interactively
❯ pda mv name name2 --safe
 ??? overwrite 'name2'? (y/n)
 >>> y

# move a key and skip all warning prompts
❯ pda mv name name2 --yes
  ok renamed name to name2
```

#### Removing

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-remove"><code>pda remove</code></a>
  </sup>
</p>

[`pda remove`](#removing) (alias: [`rm`](#removing)) deletes one or more keys. Any number of keys can be deleted in a single call, with keys from differing stores able to be mixed freely.

```bash
# delete a single key
pda remove kitty

# delete multiple keys at once
pda remove kitty doggy

# delete across stores
pda remove kitty secret@private
```

Exact positional keys can be combined with [glob patterns](#filtering) via `key`, `store`, and `value` to widen the scope of a deletion. Glob-matched deletions prompt for confirmation by default due to their more error-prone nature. This is configurable with `key.always_prompt_glob_delete` in the [config](#config).

```bash
# delete "kitty" and everything matching the key "?og"
❯ pda rm kitty --key "?og"
 ??? remove 'cog'? (y/n)
 ==> y
 ??? remove 'dog'? (y/n)

# delete keys matching a store and key pattern
❯ pda rm --store "temp*" --key "session*"
```

Passing `interactive` prompts before each deletion, including exact keys. This behaviour can be made permanent with `key.always_prompt_delete` in the [config](#config). Inversely, `yes` auto-accepts all confirmation prompts.

```bash
# prompt before each deletion
❯ pda rm kitty -i
 ??? remove 'kitty'? (y/n)
 ==> y

# auto-accept all prompts
❯ pda rm kitty -y
```

[Read-only](#read-only) keys cannot be deleted without explicitly passing `force`.

```bash
# remove a read-only key
❯ pda rm protected-key
FAIL cannot remove 'protected-key': key is read-only

# force-remove a read-only key
❯ pda rm protected-key --force
```

### Metadata

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-meta"><code>pda meta</code></a>,
    <a href="#ttl">TTL</a>,
    <a href="#encryption">Encryption</a>,
    <a href="#read-only">Read-Only</a>,
    <a href="#pinned">Pinned</a>
  </sup>
</p>

[`pda meta`](#metadata) can be used to view or modify metadata for a given key without touching its value. It always takes one argument: the desired key.

If no flags are passed, [`pda meta`](#metadata) will display the key's current metadata. Any flags passed can be used to modify metadata in-place: [`ttl`](#ttl), [`encrypt`](#encryption) or [`decrypt`](#encryption), [`readonly`](#read-only) or [`writable`](#read-only), and [`pin`](#pinned) or [`unpin`](#pinned). Multiple changes can be combined in a single command.

In [`pda list`](#listing) output, metadata is demonstrated via a `Meta` column. The presence of each type of metadata is marked by a character, or a dash if unset: [(e)ncrypted](#encryption), [(w)ritable](#read-only), [(t)ime-to-live](#ttl), and [(p)inned](#pinned).

```bash
# view a key's underlying metadata
❯ pda meta session
  key: session@store
  secret: false
  writable: true
  pinned: false
  expires: 59m30s

# make a key read-only
❯ pda meta session --readonly

# remove a key's expiration time
❯ pda meta session --ttl never
```

Modifying a [read-only](#read-only) key's metadata requires `force` or by first making it `writable`. A [read-only](#read-only) key can still be [pinned or unpinned](#pinned) as pin state only determines where a key is on `list output`[#listing], and does not change the actual key state.

#### TTL

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-set"><code>pda set</code></a>,
    <a href="#pda-meta"><code>pda meta</code></a>
  </sup>
</p>

Keys can be given an expiration time. Expired keys are marked for garbage collection and deleted on the next access to the [store](#stores). [TTL](#ttl) can be set at creation time via [`pda set --ttl`](#setting), or toggled later with [`pda meta --ttl`](#metadata) and [`pda edit --ttl`](#editing).

```bash
# expire after 1 hour
pda set session "123" --ttl 1h

# expire after 54 minutes and 10 seconds
pda set session2 "xyz" --ttl 54m10s

# remove an expiration time
pda meta session --ttl never
```

TTL can be displayed with [`pda list`](#listing) in the [TTL](#ttl) column, or with [`pda meta`](#metadata).

```bash
# view ttl in a store's list output
❯ pda ls
   TTL Key      Value
59m30s session  123
51m40s session2 xyz

# view the metadata of a specific key
❯ pda meta session
```

Expiration time is preserved on [`import`](#import--export) and [`export`](#import--export) and [moving or copying](#moving--copying). TTL is stored as a timestamp rather than a timer; keys with a TTL are checked on access to the store they reside in, and any with an expiry that has already passed are deleted. If a key expires while having been exported, it will be deleted on import the next time `pda` touches the file.

#### Encryption

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-set"><code>pda set</code></a>,
    <a href="#pda-meta"><code>pda meta</code></a>,
    <a href="#pda-identity"><code>pda identity</code></a>
  </sup>
</p>

[`pda set --encrypt`](#setting) encrypts values at rest using [age](https://github.com/FiloSottile/age). Values are stored on disk as age ciphertext and decrypted automatically at run-time by commands like [`pda get`](#getting) and [`pda list`](#listing) when the correct identity file is present. An X25519 [identity](#identity) is generated on first use.

By default, the only recipient for encrypted keys is your own [identity](#identity) file. Additional recipients can be added or removed via [`pda identity`](#identity).

```bash
# create a key called "api-key" and encrypt it
❯ pda set --encrypt api-key "sk-live-abc123"
 ok created identity at ~/.local/share/pda/identity.txt

# encrypt a key after editing in $EDITOR
❯ pda edit --encrypt api-key

# decrypt a key via meta
❯ pda meta --decrypt api-key
```

Because the on-disk value of an encrypted key is ciphertext, encrypted entries are safe to commit and push with [Git](#git). 

```bash
❯ pda export
{"key":"api-key","value":"YWdlLWVuY3J5cHRpb24u...","encoding":"secret"}
```

[`mv`](#moving--copying), [`cp`](#moving--copying), and [`import`](#import--export) all preserve encryption, read-only, and pinned flags. Overwriting an encrypted key by setting a new value without `--encrypt` will warn you.

```bash
# setting a new key and forgetting the "--encrypt" flag
❯ pda set api-key "oops"
WARN overwriting encrypted key 'api-key' as plaintext
hint pass --encrypt to keep it encrypted
```

If your [identity](#identity) file does not match an intended recipient of an encrypted key, the value will be inaccessible. It will display `locked` on fetch until a matching [identity](#identity) is found.

```bash
❯ pda ls
Meta Key     Value
ew-- api-key locked (identity file missing)

❯ pda get api-key
FAIL cannot get 'api-key': secret is locked (identity file missing)
```

Encrypted keys can be made the default by enabling `key.always_encrypt` in the [config](#config).

#### Read-Only

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-set"><code>pda set</code></a>,
    <a href="#pda-meta"><code>pda meta</code></a>
  </sup>
</p>

Keys marked [read-only](#read-only) are protected from accidental modification. A [read-only](#read-only) flag can be set at creation time, toggled later with [`pda meta`](#metadata), or applied alongside an [`edit`](#editing). Making a key [`writable`](#metadata) again or explicitly passing [`force`](#metadata) allows changes through. A key being made writable is a permanent change, whereas the `force` flag is a one-off.

```bash
# create a read-only key
pda set api-url "https://prod.example.com" --readonly

# set a key to read-only with meta
❯ pda meta api-url --readonly
  ok made readonly api-url

# set a key as writable with meta
❯ pda meta api-url --writable
  ok made writable api-url

# edit a key, and set as readonly on save
❯ pda edit notes --readonly
```

Read-only keys are protected from [`setting`](#setting), [`removing`](#removing), [`moving`](#moving--copying), and [`editing`](#editing). They are *not protected* from the deletion of an entire [store](#stores).

```bash
# set a new value to a read-only key
❯ pda set api-url "new value"
FAIL cannot set 'api-url': key is read-only

# force changes to a read-only key with the force flag
❯ pda set api-url "new value" --force
❯ pda remove api-url --force
❯ pda move api-url new-name --force
```

[`pda copy`](#moving--copying) can copy a read-only key freely (since the source isn't modified), and the copy preserves the read-only flag. Overwriting a read-only destination is blocked without `force`.

#### Pinned

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-set"><code>pda set</code></a>,
    <a href="#pda-meta"><code>pda meta</code></a>
  </sup>
</p>

Pinned keys sort to the top of [`pda list`](#listing) output, preserving alphabetical order within the pinned and unpinned groups. A pin can be set at creation time, toggled with [`pda meta`](#metadata), or applied alongside an [`edit`](#editing).

```bash
# pin a key at creation time
pda set important "remember this" --pin

# pin a key with meta
❯ pda meta todo --pin
  ok pinned todo

# unpin a key with meta
❯ pda meta todo --unpin
  ok unpinned todo

# view pinned keys in list output, at the top
❯ pda ls
Meta Key       Value
-w-p important remember this
-w-- name      Alice
-w-- other     foo
```

### Stores

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-list-stores"><code>pda list-stores</code></a>,
    <a href="#pda-move-store"><code>pda move-store</code></a>,
    <a href="#pda-remove-store"><code>pda remove-store</code></a>
  </sup>
</p>

Stores are saved on disk as NDJSON files. `pda` supports any number of stores, and creating them is automatic. If a key is created with a `@STORE` suffix, and the named store does not already exist, it will be created automatically to support the new key.

[`pda list-stores`](#stores) (alias: [`lss`](#stores)) shows all stores with their respective key counts and file sizes. Passing `short` prints only the store names.

```bash
# list all stores
❯ pda list-stores
Keys  Size Store
   2 1.8k @birthdays
  12 4.2k @store

# list all store names
❯ pda list-stores --short
@birthdays
@store
```

[`pda move-store`](#stores) (alias: [`mvs`](#stores)) renames a store. Passing `copy` keeps the source intact.

```bash
# rename a store
pda move-store birthdays bdays

# copy a store
pda move-store birthdays bdays --copy
```

[`pda remove-store`](#stores) (alias: [`rms`](#stores)) deletes a store. A confirmation prompt is shown by default (configurable with `store.always_prompt_delete` in the [config](#config)). Deleting an entire store does not require unsetting [read-only](#read-only) on contained keys: if a [read-only](#read-only) key is within a store, it **will be deleted** if the store is removed.

```bash
# delete a store
pda remove-store birthdays
```

As with changeful key operations, store commands support `interactive` and `safe` flags where they make sense. Moving or removing a store interactively will generate a confirmation prompt if anything would be lost by the action being taken. The `safe` flag will prevent moving a store from ever overwriting another store.

Inversely, `yes` can be passed to bypass any confirmation prompts.

#### Import & Export

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-export"><code>pda export</code></a>,
    <a href="#pda-import"><code>pda import</code></a>
  </sup>
</p>

[`pda export`](#import--export) dumps entries as NDJSON (it is functionally an alias for `list --format ndjson`). The [filtering](#filtering) flags `key`, `value`, and `store` all work with exports. It shares functionality with [`pda list`](#listing) in regard to which stores get exported: if `list.always_show_all_stores` is set and no store name is specified as an argument, all stores will be exported.

```bash
# export everything
pda export > my_backup

# export only matching keys
pda export --key "a*"

# export only entries whose values contain a URL
pda export --value "**https**"
```

[`pda import`](#import--export) restores entries from an NDJSON dump. The default behaviour for an import is to merge with any existing stores of the same name. To completely replace existing stores instead of merging, `drop` can be passed.

[Importing](#import--export) takes one or zero arguments. On export each key saves the name of the store it came from in its metadata; on import, by default, each key will be returned to that same store. If a store name is passed as an argument to [`pda import`](#import--export), this behaviour will be overriden and all keys will be imported into the specified store.

As with [exporting](#import--export), `key`, `value`, and `store` flags can be passed to filter which keys will be imported from the input file.

```bash
# entries are routed to their original stores
pda import -f my_backup
#   ok restored 5 entries

# force all entries into a single store
pda import mystore -f my_backup
#   ok restored 5 entries into @mystore

# read from stdin
pda import < my_backup
```

`interactive` can be passed to [`pda import`](#import--export) to prompt on potential overwrite, and is generally recommended if an import is ever being routed to a specific store, as it is likely to cause collisions.

[`pda export`](#import--export) encodes [binary data](#binary-data) as base64. All [metadata](#metadata) is preserved through export and import.

### Templates

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#getting"><code>pda get</code></a>,
    <a href="#running"><code>pda run</code></a>
  </sup>
</p>

Values support Go's [`text/template`](https://pkg.go.dev/text/template) syntax. Templates are evaluated on [`pda get`](#getting) and [`pda run`](#running).

`text/template` is a Turing-complete templating library that supports pipelines, nested templates, conditionals, loops, and more. Actions are given with `{{ action }}` syntax. To better accomodate `text/template`, `pda` adds a small set of built-in functions on top of the standard library.

These same functions are also available in `git.default_commit_message` templates, in addition to `summary`, which returns the action that triggered the commit (e.g. "set foo", "removed bar").

`no-template` can be passed to output a raw value without resolving the template.

#### Basic Substitution

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>
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

`int` parses a variable as an integer. Useful mostly for loops or arithmetic.

```bash
❯ pda set number "{{ int .N }}"
❯ pda get number N=3
3

# using "int" in a loop
❯ pda set meows "{{ range int .COUNT }}meow! {{ end }}"
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

`shell` executes a command and returns its stdout. Commands executed by the `shell` function are executed by `$SHELL`. If it is somehow unset, it defaults to using `/usr/sh`.

```bash
❯ pda set rev '{{ shell "git rev-parse --short HEAD" }}'
❯ pda get rev
a1b2c3d

❯ pda set today '{{ shell "date +%Y-%m-%d" }}'
❯ pda get today
2025-06-15
```

#### `pda`

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#templates">Templates</a>
  </sup>
</p>

`pda` returns the output of [`pda get`](#getting) on a key.

```bash
# use the value of "base_url" in another key
❯ pda set base_url "https://api.example.com"
❯ pda set endpoint '{{ pda "base_url" }}/users/{{ require .ID }}'
❯ pda get endpoint ID=42
https://api.example.com/users/42

# use the value of a key from another store
❯ pda set host@urls "https://example.com"
❯ pda set api '{{ pda "host@urls" }}/api'
❯ pda get api
https://example.com/api
```

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

[`key`](#filtering), [`value`](#filtering), and [`store`](#filtering) flags can be used to filter entries via globs. All three flags are repeatable, with results matching one-or-more of the patterns per flag. When multiple flags are combined, results must satisfy all of them (AND across flags, OR within the same flag).

These filters work with [`pda list`](#listing), [`pda export`](#import--export), [`pda import`](#import--export), and [`pda remove`](#removing).

[`gobwas/glob`](https://github.com/gobwas/glob) is used for matching. The default separators are `/-_.@:` and space. For a detailed guide to globbing, I highly recommend taking a look at the documentation there directly.

#### Glob Patterns

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#filtering">Filtering</a>
  </sup>
</p>

`*` wildcards a word or series of characters, stopping at separator boundaries.

```bash
# list all store contents
❯ pda ls
cat
mouse house
foo.bar.baz

# match any single-word key
❯ pda ls --key "*"
cat

# match any two-word key
❯ pda ls --key "* *"
mouse house

# match any key starting with "foo." and ending with ".baz", with one word between
❯ pda ls --key "foo.*.baz"
foo.bar.baz
```

`**` super-wildcards ignore word boundaries.

```bash
# match anything beginning with "foo"
❯ pda ls --key "foo**"
foo.bar.baz
```

`?` matches a single character:

```bash
# match anything beginning with any letter, and ending with "og"
❯ pda ls --key "?og"
dog
cog
```

`[abc]` matches one of the characters in the brackets.

```bash
# match anything beginning with "d" or "c", and ending with "og"
❯ pda ls --key "[dc]og"
dog
cog

# negate with '!'
❯ pda ls --key "[!dc]og"
bog
```

`[a-c]` matches a range:

```bash
# match anything beginning with "a" to "g", and ending with "ag"
❯ pda ls --key "[a-g]ag"
bag
gag

# negate with '!'
❯ pda ls --key "[!a-g]ag"
wag
```

#### Filtering by Key

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#filtering">Filtering</a>,
    <a href="#listing"><code>pda list</code></a>
  </sup>
</p>

[`key`](#filtering) filters entries by key name. Multiple `key` patterns are OR'd. An entry matches if it matches any of them.

```bash
pda ls --key "db*"
pda ls --key "session*" --key "token*"
```


#### Filtering by Value

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#filtering">Filtering</a>,
    <a href="#listing"><code>pda list</code></a>
  </sup>
</p>

[`value`](#filtering) filters by value. Multiple `value` patterns are OR'd.

```bash
❯ pda ls --value "**localhost**"
❯ pda ls --value "**world**" --value "42"
```

Locked (encrypted without an available identity) and non-UTF-8 (binary) entries are silently excluded from `value` matching.

#### Filtering by Store

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#filtering">Filtering</a>,
    <a href="#listing"><code>pda list</code></a>
  </sup>
</p>

[`store`](#filtering) filters by store name. Multiple `store` patterns are OR'd.

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

`key`, `value`, and `store` filters can be combined. Results must match at least one of each category of filter used. For example, checking for `key` and two different `value` globs on the same filter: the results must match `key` and at least one of the two `value` globs; the results do not need to match both values.

```bash
pda ls --key "db*" --value "**localhost**"
```

Globs can be combined to create some deeply complex queries. For example, [`key`](#filtering) can be combined with exact positional args on [`rm`](#removing) to remove exactly the "cat" key, and any keys beginning with "cat", "dog", or "mouse" followed by zero-or-more additional words.

```bash
pda rm cat --key "{mouse,[cd]og}**"
#  ??? remove 'cat'? (y/n)
#  ==> y
#  ??? remove 'mouse trap'? (y/n)
# ...
```

### Binary Data

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#setting"><code>pda set</code></a>,
    <a href="#getting"><code>pda get</code></a>,
    <a href="#listing"><code>pda list</code></a>
  </sup>
</p>

`pda!` supports all binary data. Values can be read from a file with `--file` or piped in via stdin. Retrieval works the same way — pipe or redirect the output to get the raw bytes.

```bash
# store binary data from a file
pda set logo < logo.png
pda set logo -f logo.png

# retrieve binary data
pda get logo > output.png
```

On a TTY, [`get`](#getting) and [`list`](#listing) show a summary instead of printing raw bytes (which can cause undefined terminal behaviour). In a non-TTY setting (piped or redirected), the raw bytes are returned as expected. Passing `base64` provides a safe way to view binary data in a terminal.

```bash
# TTY shows a summary
❯ pda get logo
(binary: 4.2 KB, image/png)

# base64 view
❯ pda get logo --base64
iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADklEQVQI12...
```

[`pda export`](#import--export) encodes binary data as base64 in the NDJSON, and [`pda edit`](#editing) presents binary values as base64 for editing and decodes them back on save.

```bash
❯ pda export
{"key":"logo","value":"89504E470D0A1A0A0000000D4948445200000001000000010802000000","encoding":"base64"}
```

### Git

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-init"><code>pda init</code></a>,
    <a href="#pda-sync"><code>pda sync</code></a>,
    <a href="#config">Config</a>
  </sup>
</p>

`pda!` supports automatic version control backed by Git, either in a local-only repository or by initialising from a remote.

#### Init

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-init"><code>pda init</code></a>
  </sup>
</p>

[`pda init`](#git) initialises version control. With no arguments it creates a local-only repository in the data directory. Pass a remote URL to clone from an existing repository instead.

```bash
# initialise an empty local repository
pda init

# or clone from an existing remote
pda init https://github.com/llywelwyn/my-repository
```

Passing `clean` removes any existing `.git` directory first, useful for reinitialising or switching remotes.

```bash
pda init --clean
pda init https://github.com/llywelwyn/my-repository --clean
```

#### Sync

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-sync"><code>pda sync</code></a>
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

#### Auto-Commit & Auto-Push

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-config"><code>pda config</code></a>
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
    <a href="#pda-identity"><code>pda identity</code></a>,
    <a href="#encryption">Encryption</a>
  </sup>
</p>

[`pda identity`](#identity) (alias: [`id`](#identity)) manages the age encryption identity used for [encryption](#encryption).

#### Viewing Identity

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-identity"><code>pda identity</code></a>
  </sup>
</p>

With no flags, [`pda identity`](#identity) shows your public key, identity file path, and any additional recipients. Passing `path` prints only the identity file path, useful for scripting.

```bash
❯ pda identity
  ok pubkey  age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p
  ok identity  ~/.config/pda/identity.txt

❯ pda identity --path
~/.config/pda/identity.txt
```

#### Creating an Identity

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-identity"><code>pda identity</code></a>
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
    <a href="#pda-identity"><code>pda identity</code></a>
  </sup>
</p>

By default, secrets are encrypted only for your own identity. To encrypt for additional recipients (e.g. a teammate or another device), use [`--add-recipient`](#identity) with their age public key. All existing secrets are automatically re-encrypted for every recipient:

```bash
❯ pda identity --add-recipient age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p
  ok re-encrypted api-key
  ok added recipient age1ql3z...
  ok re-encrypted 1 secret(s)
```

Removing a recipient with `--remove-recipient` re-encrypts all secrets without their key. Additional recipients are shown in the default identity display.

```bash
pda identity --remove-recipient age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p

❯ pda identity
  ok pubkey     age1abc...
  ok identity   ~/.local/share/pda/identity.txt
  ok recipient  age1ql3z...
```

### Config

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-config"><code>pda config</code></a>,
    <a href="#pda-doctor"><code>pda doctor</code></a>
  </sup>
</p>

Config is stored at `~/.config/pda/config.toml` (Linux/macOS) or `%LOCALAPPDATA%/pda/config.toml` (Windows). All values have sensible defaults, so a config file is entirely optional.

#### Config Commands

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-config"><code>pda config</code></a>
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

#### Example config.toml

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#config">Config</a>,
    <a href="#pda-config"><code>pda config</code></a>
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
    <a href="#pda-doctor"><code>pda doctor</code></a>
  </sup>
</p>

`pda!` respects a small set of environment variables for overriding paths and tools. These are primarily useful for isolating stores across environments or for scripting.

`PDA_CONFIG` overrides the config directory — `pda!` will look for `config.toml` here instead of the default XDG location. `PDA_DATA` overrides the data storage directory where stores and the Git repository live. Default data locations follow XDG conventions: `~/.local/share/pda/` on Linux, `~/Library/Application Support/pda/` on macOS, and `%LOCALAPPDATA%/pda/` on Windows.

```bash
# use an alternative config directory
PDA_CONFIG=/tmp/config/ pda set key value

# use an alternative data directory
PDA_DATA=/tmp/stores pda set key value
```

`EDITOR` is used by [`pda edit`](#editing) and [`pda config edit`](#config) to open values in a text editor. Must be set for these commands to work. `SHELL` is used by [`pda run`](#running) (or [`pda get --run`](#getting)) for command execution, falling back to `/bin/sh` if unset.

```bash
EDITOR=nvim pda edit mykey
```

### Doctor

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#config">Config</a>,
    <a href="#environment">Environment</a>
  </sup>
</p>

[`pda doctor`](#doctor) runs a set of health checks against your environment, covering installed tools, config validity, store integrity, and Git status.

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

### Version

<p>
  <sup>
    <a href="#overview">↑</a> ·
    <a href="#pda-version"><code>pda version</code></a>
  </sup>
</p>

[`pda version`](#version) displays the current version. Passing `short` prints just the release string without ASCII art, useful for scripting. `pda!` uses calendar versioning: `YYYY.WW`. ASCII art can be permanently disabled with `display_ascii_art = false` in [config](#config).

```bash
# display the full version output
pda version

# or just the release
❯ pda version --short
pda! 2025.52 Christmas release
```

### Help

<p>
  <sup>
    <a href="#overview">↑</a>
  </sup>
</p>

<div align="center">
  <a href="#pda-set"><code>set</code></a>&nbsp;·
  <a href="#pda-get"><code>get</code></a>&nbsp;·
  <a href="#pda-run"><code>run</code></a>&nbsp;·
  <a href="#pda-list"><code>list</code></a>&nbsp;·
  <a href="#pda-edit"><code>edit</code></a>&nbsp;·
  <a href="#pda-move"><code>move</code></a>&nbsp;·
  <a href="#pda-copy"><code>copy</code></a>&nbsp;·
  <a href="#pda-remove"><code>remove</code></a>&nbsp;·
  <a href="#pda-meta"><code>meta</code></a>&nbsp;·
  <a href="#pda-identity"><code>identity</code></a>&nbsp;·
  <a href="#pda-export"><code>export</code></a>&nbsp;·
  <a href="#pda-import"><code>import</code></a>&nbsp;·
  <a href="#pda-list-stores"><code>list-stores</code></a>&nbsp;·
  <a href="#pda-move-store"><code>move-store</code></a>&nbsp;·
  <a href="#pda-remove-store"><code>remove-store</code></a>&nbsp;·
  <a href="#pda-init"><code>init</code></a>&nbsp;·
  <a href="#pda-sync"><code>sync</code></a>&nbsp;·
  <a href="#pda-git"><code>git</code></a>&nbsp;·
  <a href="#pda-config"><code>config</code></a>&nbsp;·
  <a href="#pda-doctor"><code>doctor</code></a>&nbsp;·
  <a href="#pda-version"><code>version</code></a>
</div>

#### `pda set`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#setting">Setting</a>
  </sup>
</p>

```text
Set a key to a given value or stdin. Optionally specify a store.

Pass --encrypt to encrypt the value at rest using age. An identity file
is generated automatically on first use.

PDA supports parsing Go templates. Actions are delimited with {{ }}.

For example:
  'Hello, {{ .NAME }}'                 can be substituted with NAME="John Doe".
  'Hello, {{ env "USER" }}'            will fetch the USER env variable.
  'Hello, {{ default "World" .NAME }}' will default to World if NAME is blank.
  'Hello, {{ require .NAME }}'         will error if NAME is blank.
  '{{ enum .NAME "Alice" "Bob" }}'     allows only NAME=Alice or NAME=Bob.

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

#### `pda get`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#getting">Getting</a>,
    <a href="#templates">Templates</a>
  </sup>
</p>

```text
Get the value of a key. Optionally specify a store.

{{ .TEMPLATES }} can be filled by passing TEMPLATE=VALUE as an
additional argument after the initial KEY being fetched.

For example:
	pda set greeting 'Hello, {{ .NAME }}!'
	pda get greeting NAME=World

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

#### `pda run`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#running">Running</a>,
    <a href="#templates">Templates</a>
  </sup>
</p>

```text
Get the value of a key and execute it as a shell command. Optionally specify a store.

{{ .TEMPLATES }} can be filled by passing TEMPLATE=VALUE as an
additional argument after the initial KEY being fetched.

For example:
	pda set greeting 'Hello, {{ .NAME }}!'
	pda run greeting NAME=World

Usage:
  pda run KEY[@STORE] [flags]

Flags:
  -b, --base64        view binary data as base64
  -h, --help          help for run
      --no-template   directly output template syntax
```

#### `pda list`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#listing">Listing</a>,
    <a href="#filtering">Filtering</a>
  </sup>
</p>

```text
List the contents of all stores.

By default, list shows entries from every store. Pass a store name as a
positional argument to narrow to a single store, or use --store/-s with a
glob pattern to filter by store name.

Use --key/-k and --value/-v to filter by key or value glob, and --store/-s
to filter by store name. All filters are repeatable and OR'd within the
same flag.

Usage:
  pda list [STORE] [flags]

Aliases:
  list, ls

Flags:
  -a, --all             list across all stores
  -b, --base64          view binary data as base64
  -c, --count           print only the count of matching entries
  -o, --format format   output format (table|tsv|csv|markdown|html|ndjson|json)
  -f, --full            show full values without truncation
  -h, --help            help for list
  -k, --key strings     filter keys with glob pattern (repeatable)
      --no-header       suppress the header row
      --no-keys         suppress the key column
      --no-meta         suppress the meta column
      --no-size         suppress the size column
      --no-store        suppress the store column
      --no-ttl          suppress the TTL column
      --no-values       suppress the value column
  -s, --store strings   filter stores with glob pattern (repeatable)
  -v, --value strings   filter values with glob pattern (repeatable)
```

#### `pda edit`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#editing">Editing</a>
  </sup>
</p>

```text
Open a key's value in $EDITOR. If the key doesn't exist, opens an
empty file — saving non-empty content creates the key.

Binary values are presented as base64 for editing and decoded back on save.

Metadata flags (--ttl, --encrypt, --decrypt) can be passed alongside the edit
to modify metadata in the same operation.

Usage:
  pda edit KEY[@STORE] [flags]

Aliases:
  edit, e

Flags:
  -d, --decrypt            decrypt the value (store as plaintext)
  -e, --encrypt            encrypt the value at rest
      --force              bypass read-only protection
  -h, --help               help for edit
      --pin                pin the key (sorts to top in list)
      --preserve-newline   keep trailing newlines added by the editor
      --readonly           mark the key as read-only
      --ttl string         set expiry (e.g. 30m, 2h) or 'never' to clear
      --unpin              unpin the key
      --writable           clear the read-only flag
```

#### `pda move`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#moving--copying">Moving & Copying</a>
  </sup>
</p>

```text
Move a key

Usage:
  pda move FROM[@STORE] TO[@STORE] [flags]

Aliases:
  move, mv

Flags:
      --copy          copy instead of move (keeps source)
      --force         bypass read-only protection
  -h, --help          help for move
  -i, --interactive   prompt before overwriting destination
      --safe          do not overwrite if the destination already exists
  -y, --yes           skip all confirmation prompts
```

#### `pda copy`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#moving--copying">Moving & Copying</a>
  </sup>
</p>

```text
Make a copy of a key

Usage:
  pda copy FROM[@STORE] TO[@STORE] [flags]

Aliases:
  copy, cp

Flags:
      --force         bypass read-only protection
  -h, --help          help for copy
  -i, --interactive   prompt before overwriting destination
      --safe          do not overwrite if the destination already exists
  -y, --yes           skip all confirmation prompts
```

#### `pda remove`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#removing">Removing</a>,
    <a href="#filtering">Filtering</a>
  </sup>
</p>

```text
Delete one or more keys

Usage:
  pda remove KEY[@STORE] [KEY[@STORE] ...] [flags]

Aliases:
  remove, rm

Flags:
      --force           bypass read-only protection
  -h, --help            help for remove
  -i, --interactive     prompt yes/no for each deletion
  -k, --key strings     delete keys matching glob pattern (repeatable)
  -s, --store strings   target stores matching glob pattern (repeatable)
  -v, --value strings   delete entries matching value glob pattern (repeatable)
  -y, --yes             skip all confirmation prompts
```

#### `pda meta`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#metadata">Metadata</a>,
    <a href="#ttl">TTL</a>,
    <a href="#encryption">Encryption</a>,
    <a href="#read-only">Read-Only</a>,
    <a href="#pinned">Pinned</a>
  </sup>
</p>

```text
View or modify metadata (TTL, encryption, read-only, pinned) for a key
without changing its value.

With no flags, displays the key's current metadata. Pass flags to modify.

Usage:
  pda meta KEY[@STORE] [flags]

Flags:
  -d, --decrypt      decrypt the value (store as plaintext)
  -e, --encrypt      encrypt the value at rest
      --force        bypass read-only protection for metadata changes
  -h, --help         help for meta
      --pin          pin the key (sorts to top in list)
      --readonly     mark the key as read-only
      --ttl string   set expiry (e.g. 30m, 2h) or 'never' to clear
      --unpin        unpin the key
      --writable     clear the read-only flag
```

#### `pda identity`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#identity">Identity</a>,
    <a href="#encryption">Encryption</a>
  </sup>
</p>

```text
Show or create the age encryption identity

Usage:
  pda identity [flags]

Aliases:
  identity, id

Flags:
      --add-recipient string      add an age public key as an additional encryption recipient
  -h, --help                      help for identity
      --new                       generate a new identity (errors if one already exists)
      --path                      print only the identity file path
      --remove-recipient string   remove an age public key from the recipient list
```

#### `pda export`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#import--export">Import & Export</a>
  </sup>
</p>

```text
Export store as NDJSON (alias for list --format ndjson)

Usage:
  pda export [STORE] [flags]

Flags:
  -h, --help            help for export
  -k, --key strings     filter keys with glob pattern (repeatable)
  -s, --store strings   filter stores with glob pattern (repeatable)
  -v, --value strings   filter values with glob pattern (repeatable)
```

#### `pda import`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#import--export">Import & Export</a>
  </sup>
</p>

```text
Restore key/value pairs from an NDJSON dump

Usage:
  pda import [STORE] [flags]

Flags:
      --drop            drop existing entries before restoring (full replace)
  -f, --file string     path to an NDJSON dump (defaults to stdin)
  -h, --help            help for import
  -i, --interactive     prompt before overwriting existing keys
  -k, --key strings     restore keys matching glob pattern (repeatable)
  -s, --store strings   restore entries from stores matching glob pattern (repeatable)
```

#### `pda list-stores`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#stores">Stores</a>
  </sup>
</p>

```text
List all stores

Usage:
  pda list-stores [flags]

Aliases:
  list-stores, lss

Flags:
  -h, --help        help for list-stores
      --no-header   suppress the header row
      --short       only print store names
```

#### `pda move-store`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#stores">Stores</a>
  </sup>
</p>

```text
Rename a store

Usage:
  pda move-store FROM TO [flags]

Aliases:
  move-store, mvs

Flags:
      --copy          copy instead of move (keeps source)
  -h, --help          help for move-store
  -i, --interactive   prompt before overwriting destination
      --safe          do not overwrite if the destination store already exists
  -y, --yes           skip all confirmation prompts
```

#### `pda remove-store`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#stores">Stores</a>
  </sup>
</p>

```text
Delete a store

Usage:
  pda remove-store STORE [flags]

Aliases:
  remove-store, rms

Flags:
  -h, --help          help for remove-store
  -i, --interactive   prompt yes/no for each deletion
  -y, --yes           skip all confirmation prompts
```

#### `pda init`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#init">Init</a>,
    <a href="#git">Git</a>
  </sup>
</p>

```text
Initialise pda! version control

Usage:
  pda init [remote-url] [flags]

Flags:
      --clean   remove .git from stores directory before initialising
  -h, --help    help for init
```

#### `pda sync`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#sync">Sync</a>,
    <a href="#git">Git</a>
  </sup>
</p>

```text
Manually sync your stores with Git

Usage:
  pda sync [flags]

Flags:
  -h, --help             help for sync
  -m, --message string   custom commit message (defaults to timestamp)
```

#### `pda git`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#git">Git</a>
  </sup>
</p>

```text
Run any arbitrary command. Use with caution.

The Git repository lives directly in the data directory
("PDA_DATA"). Store files (*.ndjson) are tracked by Git as-is.

If you manually modify files without using the built-in
commands, you may desync your repository.

Generally prefer "pda sync".

Usage:
  pda git [args...] [flags]

Flags:
  -h, --help   help for git
```

#### `pda config`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#config">Config</a>
  </sup>
</p>

```text
View and modify configuration

Usage:
  pda config [command]

Available Commands:
  edit        Open config file in $EDITOR
  get         Print a configuration value
  init        Generate default config file
  list        List all configuration values
  path        Print config file path
  set         Set a configuration value

Flags:
  -h, --help   help for config

Use "pda config [command] --help" for more information about a command.
```

#### `pda doctor`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#doctor">Doctor</a>
  </sup>
</p>

```text
Check environment health

Usage:
  pda doctor [flags]

Flags:
  -h, --help   help for doctor
```

#### `pda version`

<p>
  <sup>
    <a href="#help">↑</a> ·
    See also:
    <a href="#version">Version</a>
  </sup>
</p>

```text
Display pda! version

Usage:
  pda version [flags]

Flags:
  -h, --help    help for version
      --short   print only the version string
```

### License

<p>
  <sup>
    <a href="#overview">↑</a>
  </sup>
</p>

MIT — see [LICENSE](LICENSE).
