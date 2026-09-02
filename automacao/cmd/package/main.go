package main

import (
	"archive/zip"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: package BUILD_DIR")
		os.Exit(2)
	}
	root, err := filepath.Abs(os.Args[1])
	if err != nil {
		fatal(err)
	}
	for _, name := range []string{"organizer", "worker"} {
		if err := makeZip(filepath.Join(root, name, "bootstrap"), filepath.Join(root, name+".zip")); err != nil {
			fatal(err)
		}
	}
	checksums, err := os.Create(filepath.Join(root, "SHA256SUMS"))
	if err != nil {
		fatal(err)
	}
	for _, name := range []string{"organizer/bootstrap", "worker/bootstrap", "organizer.zip", "worker.zip"} {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil {
			fatal(err)
		}
		fmt.Fprintf(checksums, "%x  %s\n", sha256.Sum256(body), name)
	}
	if err := checksums.Close(); err != nil {
		fatal(err)
	}
}

func makeZip(source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(destination)
	if err != nil {
		return err
	}
	zw := zip.NewWriter(out)
	header := &zip.FileHeader{Name: "bootstrap", Method: zip.Deflate, Modified: time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)}
	header.SetMode(0o755)
	entry, err := zw.CreateHeader(header)
	if err == nil {
		_, err = io.Copy(entry, in)
	}
	if closeErr := zw.Close(); err == nil {
		err = closeErr
	}
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	return err
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
