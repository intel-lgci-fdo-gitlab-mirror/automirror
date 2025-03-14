# Automirror

Scripts and configuration for automatic mirroring between the specified
repositories and branches. All code should run automatically in GitHub Actions.

If you are having trouble with the initial mirror push, you might be hitting a server-side push limit/timeout.
See [GitHub docs](https://docs.github.com/en/get-started/using-git/troubleshooting-the-2-gb-push-limit) for possible workarounds.

It is recommended to fork kernel repos between each other and within the same network
to share as many Git objects as possible under the hood. This decreases the amount
of data each push needs to process and lowers the storage costs for the forge.
