<!-- SPDX-License-Identifier: Apache-2.0 -->

# Releasing

Releases are tagged `vMAJOR.MINOR.PATCH`. GitHub Actions builds the wheel and
sdist, verifies reproducibility and package boundaries, generates SPDX and
conformance evidence, and attaches immutable digests to the GitHub release.

The first PyPI publication is intentionally not automated until an owner has
established the namespace and trusted publisher. GitHub release artifacts are
the ordinary public distribution channel until that one-time gate is complete.
