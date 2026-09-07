// imagebuild is build-only tooling. It copies the ELF DT_NEEDED closure from
// checksum-pinned packages, never from host /lib, into a scratch image root.
package main

import (
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

var tools = []string{"pg_basebackup", "pg_verifybackup", "pg_combinebackup", "pg_waldump", "pg_controldata", "psql"}

type entry struct {
	Path   string   `json:"path"`
	Source string   `json:"package_path"`
	SHA256 string   `json:"sha256"`
	Needed []string `json:"needed,omitempty"`
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func copyFile(src, dst string, mode os.FileMode) {
	must(os.MkdirAll(filepath.Dir(dst), 0755))
	in, err := os.Open(src)
	must(err)
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	must(err)
	_, err = io.Copy(out, in)
	must(err)
	must(out.Close())
}

func main() {
	if len(os.Args) != 3 {
		panic("usage: imagebuild PACKAGE_ROOT OUTPUT_ROOT (new)")
	}
	root, out := os.Args[1], os.Args[2]
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		panic("output must not exist")
	}
	must(os.MkdirAll(out, 0755))
	seen := map[string]bool{}
	var inventory []entry
	var add func(string, string)
	add = func(src, dest string) {
		if seen[dest] {
			return
		}
		seen[dest] = true
		resolved, err := filepath.EvalSymlinks(src)
		must(err)
		rel, err := filepath.Rel(root, resolved)
		must(err)
		if !filepath.IsLocal(rel) {
			panic("library escapes package root: " + src)
		}
		f, err := elf.Open(resolved)
		must(err)
		needed, err := f.ImportedLibraries()
		must(err)
		var interp string
		for _, p := range f.Progs {
			if p.Type == elf.PT_INTERP {
				b, err := io.ReadAll(p.Open())
				must(err)
				interp = string(b[:len(b)-1])
			}
		}
		must(f.Close())
		copyFile(resolved, filepath.Join(out, dest), 0755)
		b, err := os.ReadFile(resolved)
		must(err)
		h := sha256.Sum256(b)
		inventory = append(inventory, entry{dest, rel, hex.EncodeToString(h[:]), needed})
		if interp != "" {
			add(filepath.Join(root, interp), interp)
		}
		for _, name := range needed {
			found := false
			for _, dir := range []string{"usr/lib/x86_64-linux-gnu", "lib/x86_64-linux-gnu", "usr/lib64", "lib64"} {
				p := filepath.Join(root, dir, name)
				if _, err := os.Stat(p); err == nil {
					add(p, "/lib/x86_64-linux-gnu/"+name)
					found = true
					break
				}
			}
			if !found {
				panic("missing pinned library: " + name)
			}
		}
	}
	for _, name := range tools {
		add(filepath.Join(root, "usr/lib/postgresql/18/bin", name), "/usr/lib/postgresql/18/bin/"+name)
	}
	sort.Slice(inventory, func(i, j int) bool { return inventory[i].Path < inventory[j].Path })
	b, err := json.MarshalIndent(inventory, "", "  ")
	must(err)
	must(os.MkdirAll(filepath.Join(out, "usr/share/cnpg-backup"), 0755))
	must(os.WriteFile(filepath.Join(out, "usr/share/cnpg-backup/native-files.json"), append(b, '\n'), 0644))
	fmt.Printf("copied six tools and loader/library closure: %d ELF files\n", len(inventory))
}
