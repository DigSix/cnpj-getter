package pipeline

import (
	"archive/zip"
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const webdavBase = "https://dados-hom.receitafederal.gov.br/public.php/webdav"

// DownloadMonth lists the official ZIPs and downloads only missing files.
func DownloadMonth(ctx context.Context, p Paths, token string) error {
	url := webdavBase + "/Dados/Cadastros/CNPJ/" + p.Month + "/"
	body := []byte(`<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:allprop/></d:propfind>`)
	req, _ := http.NewRequestWithContext(ctx, "PROPFIND", url, strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(token+":")))
	req.Header.Set("Depth", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("WebDAV listing returned %s", resp.Status)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var prop struct {
		Responses []struct {
			Href string `xml:"href"`
		} `xml:"response"`
	}
	if err := xml.Unmarshal(data, &prop); err != nil {
		return err
	}
	for _, item := range prop.Responses {
		name := filepath.Base(strings.TrimSuffix(item.Href, "/"))
		if !relevantZip(name) {
			continue
		}
		out := filepath.Join(p.Raw(), name)
		if info, e := os.Stat(out); e == nil && info.Size() > 0 {
			continue
		}
		if err := downloadFile(ctx, url+name, out, req.Header.Get("Authorization")); err != nil {
			return fmt.Errorf("download %s: %w", name, err)
		}
	}
	return nil
}

func downloadFile(ctx context.Context, url, out, auth string) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("Authorization", auth)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s", resp.Status)
	}
	tmp := out + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, resp.Body)
	closeErr := f.Close()
	if copyErr != nil {
		os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		os.Remove(tmp)
		return closeErr
	}
	return os.Rename(tmp, out)
}

func relevantZip(name string) bool {
	n := strings.ToLower(name)
	return strings.HasSuffix(n, ".zip") && (strings.HasPrefix(n, "estabelecimentos") || strings.HasPrefix(n, "empresas") || strings.HasPrefix(n, "cnae") || strings.HasPrefix(n, "municipios"))
}

func ExtractMonth(p Paths) error {
	entries, err := os.ReadDir(p.Raw())
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasSuffix(strings.ToLower(entry.Name()), ".zip") {
			continue
		}
		r, err := zip.OpenReader(filepath.Join(p.Raw(), entry.Name()))
		if err != nil {
			return err
		}
		for _, file := range r.File {
			target := filepath.Join(p.Extracted(), filepath.Base(file.Name))
			if err := extractFile(file, target); err != nil {
				r.Close()
				return err
			}
		}
		r.Close()
	}
	return nil
}
func extractFile(src *zip.File, target string) error {
	in, err := src.Open()
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(target)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
