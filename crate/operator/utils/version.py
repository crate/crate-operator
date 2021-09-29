# CrateDB Kubernetes Operator
# Copyright (C) 2021 Crate.IO GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import re
from distutils.version import Version
from typing import Optional, Tuple


class CrateVersion(Version):
    """Version numbering for CrateDB releases.

    Implements the standard interface for version number classes as
    described above. A CrateDB version consists of two or three
    dot-separated numeric components, with an optional "-SNAPSHOT" tag
    on the end. Alternatively, a nightly version consists of the term "nightly"
    optionally followed by three dot-separated numeric components indicating
    the version, as well as and optional the date component for the day of the
    build in the form ``YYYYMMDD``.

    The following are valid version numbers (shown in the order that
    would be obtained by sorting according to the supplied cmp function):

    - 0.1
    - 3.2
    - 3.2.0
    - nightly-3.2.1
    - nightly-3.2.1-20190708
    - 3.2.1-SNAPSHORT
    - 3.2.1
    - 4.0.0-SNAPSHOT
    - nightly

    The following are examples of invalid version numbers:

    - 1
    - 1.2-SNAPSHOT
    - nightly-3.2
    - nightly-3.2.1-201907080200
    - nightly-3.2.1-201907080200-abc9876

    The rationale for this version numbering system will be explained
    in the distutils documentation.
    """

    version: Optional[Tuple[int, int, int]]

    major: int
    minor: int
    hotfix: int

    stable: bool
    snapshot: bool
    nightly: bool

    date: Optional[str] = None

    #: The regular expression to match a version string. Valid values are e.g.:
    #:
    #: - ``0.1``
    #: - ``0.1.2``
    #: - ``0.1.2-SNAPSHOT``
    #:
    #: Invalid values are, e.g:
    #:
    #: - ``0``
    #: - ``0.1-SNAPSHOT``
    version_re = re.compile(
        r"""
            ^  # Start of the string
            (?P<major>\d+)\.(?P<minor>\d+)  # Every version has a major.minor
            (\.(?P<hotfix>\d+)(?P<snapshot>-SNAPSHOT)?)?  # optionally hotfix
            $  # End of the string
        """,
        re.ASCII + re.VERBOSE,
    )

    #: The regular expression to match a nightly version string. Valid values
    #: are e.g.:
    #:
    #: - ``nightly``
    #: - ``nightly-0.1.2``
    #: - ``nightly-0.1.2-20190708``
    #: - ``nightly-0.1.2-201907080200-abc0987``
    #:
    #: Invalid values are, e.g:
    #:
    #: - ``nightly-0.1``
    #: - ``nightly-0.1.2-201907080200``
    nightly_version_re = re.compile(
        r"""
            ^
            (?:nightly)
            (?:
                -(?P<has_version>(?P<major>\d+)\.(?P<minor>\d+)\.(?P<hotfix>\d+))
                (?:
                    -(?P<date>\d{8})  # Match yyyymmdd
                )?
            )?$
        """,
        re.ASCII + re.VERBOSE,
    )

    def __init__(self, vstring: str):
        self.parse(vstring)

    def __repr__(self):
        return f"{self.__class__.__name__}('{self}')"

    def __str__(self):
        if self.stable:
            return ".".join(map(str, self.version))

        if self.nightly:
            vstring = "nightly"
            if self.version:
                vstring += f"-{self.major}.{self.minor}.{self.hotfix}"
                if self.date:
                    vstring += f"-{self.date}"
            return vstring

        if self.snapshot:
            return f"{self.major}.{self.minor}.{self.hotfix}-SNAPSHOT"

    def _cmp(self, other):
        if isinstance(other, str):
            other = CrateVersion(other)

        if self.version and other.version and self.version != other.version:
            # For stable or snapshot releases, as well as for nightly versions
            # when both have version information, when the numeric versions
            # don't match the prerelease stuff doesn't matter.
            if self.version < other.version:
                return -1
            else:
                return 1

        if self.stable and other.stable:
            return 0
        if self.snapshot and other.snapshot:
            return 0
        if self.nightly and other.nightly:
            if not self.version and not other.version:
                return 0
            if self.version and not other.version:
                return -1
            if not self.version and other.version:
                return 1

            assert self.version
            assert other.version

            if not self.date and not other.date:
                return 0
            if self.date and not other.date:
                return -1
            if not self.date and other.date:
                return 1

            assert self.date
            assert other.date

            if self.date < other.date:
                return -1
            if self.date > other.date:
                return 1

            return 0
        if self.nightly and (other.snapshot or other.stable):
            if self.version:
                return -1
            return 1
        if self.snapshot and other.stable:
            return -1
        if self.stable and (other.snapshot or other.nightly):
            if other.version:
                return 1
            return -1
        if self.snapshot and other.nightly:
            if other.version:
                return 1
            return -1

        assert False, "versions are not comparable"

    def __lshift__(self, other):
        assert isinstance(other, CrateVersion)

        if self.version and other.version:
            return self.version < other.version
        # at least one does not have a version number, thus being a nightly w/o
        # version thus being greater than the other if the other does have a
        # version.
        if not self.version and not other.version:
            return False
        if self.version and not other.version:
            return True

        return False

    def __rshift__(self, other):
        assert isinstance(other, CrateVersion)

        if self.version and other.version:
            return self.version > other.version
        # at least one does not have a version number, thus being a nightly w/o
        # version thus being greater than the other if the other does have a
        # version.
        if not self.version and not other.version:
            return False
        if self.version and not other.version:
            return False

        return True

    def parse(self, vstring):
        if vstring.startswith("nightly"):
            self._parse_nightly(vstring)
        else:
            self._parse_regular(vstring)

    def _parse_regular(self, vstring):
        match = self.version_re.match(vstring)
        if not match:
            raise ValueError("Invalid version number: '%s'" % vstring)

        major, minor, hotfix, snapshot = match.group(
            "major", "minor", "hotfix", "snapshot"
        )

        if hotfix:
            self.version = tuple(map(int, [major, minor, hotfix]))
        else:
            self.version = tuple(map(int, [major, minor])) + (0,)

        self.stable = False if snapshot else True
        self.nightly = False
        self.snapshot = True if snapshot else False
        self.major, self.minor, self.hotfix = self.version

    def _parse_nightly(self, vstring):
        match = self.nightly_version_re.match(vstring)
        if not match:
            raise ValueError("Invalid version number: '%s'" % vstring)

        has_version, major, minor, hotfix, date = match.group(
            "has_version", "major", "minor", "hotfix", "date"
        )

        if has_version:
            self.major, self.minor, self.hotfix = int(major), int(minor), int(hotfix)
            self.version = tuple(map(int, [self.major, self.minor, self.hotfix]))
        else:
            self.major, self.minor, self.hotfix = None, None, None
            self.version = None
        self.stable = False
        self.snapshot = False
        self.nightly = True
        self.date = date
