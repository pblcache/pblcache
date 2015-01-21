//
// Copyright (c) 2014 The pblcache Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
package tests

type MockFile struct {
	MockClose   func() error
	MockSeek    func(offset int64, whence int) (int64, error)
	MockReadAt  func(p []byte, off int64) (n int, err error)
	MockWriteAt func(p []byte, off int64) (n int, err error)
}

func NewMockFile() *MockFile {
	m := &MockFile{}
	m.MockClose = func() error { return nil }

	m.MockSeek = func(offset int64, whence int) (int64, error) {
		return 0, nil
	}

	m.MockReadAt = func(p []byte, off int64) (n int, err error) {
		return len(p), nil
	}

	m.MockWriteAt = func(p []byte, off int64) (n int, err error) {
		return len(p), nil
	}

	return m
}

func (m *MockFile) Close() error {
	return m.MockClose()
}

func (m *MockFile) Seek(offset int64, whence int) (int64, error) {
	return m.MockSeek(offset, whence)

}

func (m *MockFile) WriteAt(p []byte, off int64) (n int, err error) {
	return m.MockWriteAt(p, off)

}

func (m *MockFile) ReadAt(p []byte, off int64) (n int, err error) {
	return m.MockReadAt(p, off)
}
