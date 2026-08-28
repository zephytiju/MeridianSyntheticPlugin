<!-- SPDX-License-Identifier: Apache-2.0 -->

# Releasing

Releases are tagged `vMAJOR.MINOR.PATCH`. GitHub Actions builds the wheel and
sdist, verifies reproducibility and package boundaries, generates SPDX and
conformance evidence, and attaches immutable digests to the GitHub release.

The canonical distribution is `meridian-storage-plugin-synthetic`. Its first
PyPI publication remains gated until an owner has established that namespace
and trusted publisher. GitHub release artifacts are the ordinary public
distribution channel until that one-time gate is complete.
