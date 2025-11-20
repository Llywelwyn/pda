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

  ```bash
Available Commands:
    get         # Get a value.
    set         # Set a value.
    del         # Delete a value.
    del-db      # Delete a whole database.
    list-dbs    # List all databases.
    dump        # Export a database as NDJSON.
    restore     # Imports NDJSON into a database.
    completion  # Generate autocompletions for a specified shell.
    help        # Additional help for any command.
```


<div align="center">
  <img src="https://raw.githubusercontent.com/llywelwyn/pda/master/vhs/intro.gif"
       alt="pda template demonstration" />
</div>

<p align="center"></p><!-- spacer -->

```bash
# Get the latest release from the AUR
pacman -S pda

# Or use pda-git for the latest commit
pacman -S pda-git

# Or manually install with Go
git clone https://github.com/llywelwyn/pda
cd pda
go install
```

<p align="center"></p><!-- spacer -->
