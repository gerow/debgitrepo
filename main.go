package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/ulikunitz/xz"
	"pault.ag/go/debian/control"
	"pault.ag/go/debian/version"
)

const (
	timeLayout = "20060102T150405Z"
)

// This is all very hacky. http://bugs.debian.org/969605 proposes a way to
// improve the snapshot.d.o api to make this nicer.
func closestSnapshotTime(t time.Time) (time.Time, error) {
	hc := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	path := "https://snapshot.debian.org/archive/debian/" + t.Format(timeLayout) + "/"
	req, err := http.NewRequest("HEAD", path, nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to create HEAD request at path %q: %w", path, err)
	}
	r, err := hc.Do(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to requset HEAD for time %v: %w", t, err)
	}
	defer r.Body.Close()

	// This is incredibly unlikely.
	if r.StatusCode == http.StatusOK {
		log.Printf("received HTTP OK for timestamp %v, this is extremely unlikely, but continuing on anyway", t)
		return t, nil
	}
	if r.StatusCode != http.StatusMovedPermanently {
		// Anything else is unexpected.
		return time.Time{}, fmt.Errorf("received unexpected status %s", r.Status)
	}
	l := r.Header.Get("Location")
	if l == "" {
		return time.Time{}, fmt.Errorf("expected 301 for %v to include a Location header, it did not", t)
	}
	// We expect the value snapshot.d.o to hand back to us should look something like this:
	// https://snapshot.debian.org/archive/debian/20210801T023234Z/
	// So just parse out the path, and take the final element in the path, which should be our timestamp.
	u, err := url.Parse(l)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse URL %s: %w", l, err)
	}
	sp := strings.Split(u.Path, "/")
	if len(sp) < 2 {
		return time.Time{}, fmt.Errorf("header Location %q from HEAD req doesn't look anything like what we expect", l)
	}
	st := sp[len(sp)-2]

	return time.Parse(timeLayout, st)
}

func streamPackages(t time.Time, dist, component, arch string) (io.ReadCloser, error) {
	path := "https://snapshot.debian.org/archive/debian/" + filepath.Join(t.Format(timeLayout), "dists", dist, component, "binary-"+arch, "Packages.xz")
	resp, err := http.Get(path)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %q: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("received non-ok status %s", resp.Status)
	}

	r, err := xz.NewReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to create new xz reader: %w", err)
	}

	// Mash up our reader and closer so that we can read from the xz stream but still properly close the response body stream.
	return struct {
		io.Reader
		io.Closer
	}{
		Reader: r,
		Closer: resp.Body,
	}, nil
}

func dirForSource(src string) string {
	if strings.HasPrefix(src, "lib") {
		return path.Join(src[:4], src)
	}
	return path.Join(src[:1], src)
}

func sourceName(pkg control.BinaryIndex) string {
	src := pkg.Source
	if src == "" {
		src = pkg.Package
	}
	return strings.SplitN(src, " ", 2)[0]
}

func writeBinPkgs(wt *git.Worktree, dist, component, arch string, pkgs []control.BinaryIndex) error {
	fs := wt.Filesystem
	src := sourceName(pkgs[0])
	loc := path.Join(dist, component, dirForSource(src), "binary-"+arch)
	if err := fs.MkdirAll(path.Dir(loc), 0777); err != nil {
		return fmt.Errorf("failed to create directories for %s: %w", src, err)
	}
	f, err := fs.OpenFile(loc, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("failed to open %q: %w", loc, err)
	}
	defer f.Close()
	enc, err := control.NewEncoder(f)
	if err != nil {
		return fmt.Errorf("failed to create encoder for %q: %w", src, err)
	}
	for _, p := range pkgs {
		if err := enc.Encode(p); err != nil {
			return fmt.Errorf("failed to encode pkg %q: %w", p.Package, err)
		}
	}

	return nil
}

func run() error {
	end := time.Now()
	start := end.Add(-30 * 24 * time.Hour)
	t, err := closestSnapshotTime(start)
	if err != nil {
		return fmt.Errorf("failed to determine snapshot time %v: %w", start, err)
	}
	start = t
	fmt.Print(t)

	var (
		dist      = "sid"
		component = "main"
		arch      = "amd64"
		repoPath  = "/home/gerow/debgit-test"
	)

	repo, err := git.PlainInit(repoPath, false)
	if err != nil {
		return fmt.Errorf("failed to init repo: %w", err)
	}

	for {
		// XXX(gerow) we're skipping the first one now
		t2 := t.Add(6 * time.Hour)
		for {
			tCandidate, err := closestSnapshotTime(t2)
			if err != nil {
				return fmt.Errorf("failed to get next snapshot time: %w", err)
			}
			if tCandidate.Equal(t) || tCandidate.Before(t) {
				t2 = t2.Add(6 * time.Hour)
				continue
			} else {
				if t2.After(end) {
					goto done
				}
				break
			}
		}
		t = t2
		log.Printf("working on snapshot %v", t)

		r, err := streamPackages(t, dist, component, arch)
		if err != nil {
			return fmt.Errorf("failed to start streaming packagaes: %w", err)
		}
		defer r.Close()
		// It might be nice to be able to stream this instead, but really it isn't a big deal.
		idx, err := control.ParseBinaryIndex(bufio.NewReader(r))
		if err != nil {
			return fmt.Errorf("failed to parse binary Packages index: %w", err)
		}
		// We'll probably already be sorted somewhat like this, but just to be sure.
		// Sort by source name, package name, and then version.
		sort.Slice(idx, func(i, j int) bool {
			a, b := idx[i], idx[j]
			sa, sb := sourceName(a), sourceName(b)
			if sa != sb {
				return sa < sb
			}
			if pa, pb := a.Package, b.Package; pa != pb {
				return pa < pb
			}
			return version.Compare(a.Version, b.Version) < 0
		})

		wt, err := repo.Worktree()
		if err != nil {
			return fmt.Errorf("failed to get worktree for git repo: %w", err)
		}
		_, err = wt.Remove("/")
		if err != nil {
			return fmt.Errorf("failed to remove all: %w", err)
		}
		binPkgs := []control.BinaryIndex{idx[0]}
		src := sourceName(idx[0])
		for _, pkg := range idx[1:] {
			if pkg.Source != "" && pkg.Source == src {
				binPkgs = append(binPkgs, pkg)
			} else {
				if err := writeBinPkgs(wt, dist, component, arch, binPkgs); err != nil {
					return err
				}
				binPkgs = []control.BinaryIndex{pkg}
				src = sourceName(pkg)
			}
		}
		if err := wt.AddWithOptions(&git.AddOptions{All: true}); err != nil {
			return fmt.Errorf("failed to add: %w", err)
		}
		_, err = wt.Commit("snapshot at %s"+t.Format(timeLayout), &git.CommitOptions{})
		if err != nil {
			return fmt.Errorf("failed to commit: %w", err)
		}
	}
done:

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("failed to run: %v", err)
	}
}
