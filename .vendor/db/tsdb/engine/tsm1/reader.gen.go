// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: reader.gen.go.tmpl

package tsm1

import (
	"github.com/cnosdb/db/tsdb"
)

// ReadFloatBlockAt returns the float values corresponding to the given index entry.
func (t *TSMReader) ReadFloatBlockAt(entry *IndexEntry, vals *[]FloatValue) ([]FloatValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readFloatBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadFloatArrayBlockAt fills vals with the float values corresponding to the given index entry.
func (t *TSMReader) ReadFloatArrayBlockAt(entry *IndexEntry, vals *tsdb.FloatArray) error {
	t.mu.RLock()
	err := t.accessor.readFloatArrayBlock(entry, vals)
	t.mu.RUnlock()
	return err
}

// ReadIntegerBlockAt returns the integer values corresponding to the given index entry.
func (t *TSMReader) ReadIntegerBlockAt(entry *IndexEntry, vals *[]IntegerValue) ([]IntegerValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readIntegerBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadIntegerArrayBlockAt fills vals with the integer values corresponding to the given index entry.
func (t *TSMReader) ReadIntegerArrayBlockAt(entry *IndexEntry, vals *tsdb.IntegerArray) error {
	t.mu.RLock()
	err := t.accessor.readIntegerArrayBlock(entry, vals)
	t.mu.RUnlock()
	return err
}

// ReadUnsignedBlockAt returns the unsigned values corresponding to the given index entry.
func (t *TSMReader) ReadUnsignedBlockAt(entry *IndexEntry, vals *[]UnsignedValue) ([]UnsignedValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readUnsignedBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadUnsignedArrayBlockAt fills vals with the unsigned values corresponding to the given index entry.
func (t *TSMReader) ReadUnsignedArrayBlockAt(entry *IndexEntry, vals *tsdb.UnsignedArray) error {
	t.mu.RLock()
	err := t.accessor.readUnsignedArrayBlock(entry, vals)
	t.mu.RUnlock()
	return err
}

// ReadStringBlockAt returns the string values corresponding to the given index entry.
func (t *TSMReader) ReadStringBlockAt(entry *IndexEntry, vals *[]StringValue) ([]StringValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readStringBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadStringArrayBlockAt fills vals with the string values corresponding to the given index entry.
func (t *TSMReader) ReadStringArrayBlockAt(entry *IndexEntry, vals *tsdb.StringArray) error {
	t.mu.RLock()
	err := t.accessor.readStringArrayBlock(entry, vals)
	t.mu.RUnlock()
	return err
}

// ReadBooleanBlockAt returns the boolean values corresponding to the given index entry.
func (t *TSMReader) ReadBooleanBlockAt(entry *IndexEntry, vals *[]BooleanValue) ([]BooleanValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readBooleanBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadBooleanArrayBlockAt fills vals with the boolean values corresponding to the given index entry.
func (t *TSMReader) ReadBooleanArrayBlockAt(entry *IndexEntry, vals *tsdb.BooleanArray) error {
	t.mu.RLock()
	err := t.accessor.readBooleanArrayBlock(entry, vals)
	t.mu.RUnlock()
	return err
}

// blockAccessor abstracts a method of accessing blocks from a
// TSM file.
type blockAccessor interface {
	init() (*indirectIndex, error)
	read(key []byte, timestamp int64) ([]Value, error)
	readAll(key []byte) ([]Value, error)
	readBlock(entry *IndexEntry, values []Value) ([]Value, error)
	readFloatBlock(entry *IndexEntry, values *[]FloatValue) ([]FloatValue, error)
	readFloatArrayBlock(entry *IndexEntry, values *tsdb.FloatArray) error
	readIntegerBlock(entry *IndexEntry, values *[]IntegerValue) ([]IntegerValue, error)
	readIntegerArrayBlock(entry *IndexEntry, values *tsdb.IntegerArray) error
	readUnsignedBlock(entry *IndexEntry, values *[]UnsignedValue) ([]UnsignedValue, error)
	readUnsignedArrayBlock(entry *IndexEntry, values *tsdb.UnsignedArray) error
	readStringBlock(entry *IndexEntry, values *[]StringValue) ([]StringValue, error)
	readStringArrayBlock(entry *IndexEntry, values *tsdb.StringArray) error
	readBooleanBlock(entry *IndexEntry, values *[]BooleanValue) ([]BooleanValue, error)
	readBooleanArrayBlock(entry *IndexEntry, values *tsdb.BooleanArray) error
	readBytes(entry *IndexEntry, buf []byte) (uint32, []byte, error)
	rename(path string) error
	path() string
	close() error
	free() error
}

func (m *mmapAccessor) readFloatBlock(entry *IndexEntry, values *[]FloatValue) ([]FloatValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeFloatBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readFloatArrayBlock(entry *IndexEntry, values *tsdb.FloatArray) error {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return ErrTSMClosed
	}

	err := DecodeFloatArrayBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	return err
}

func (m *mmapAccessor) readIntegerBlock(entry *IndexEntry, values *[]IntegerValue) ([]IntegerValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeIntegerBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readIntegerArrayBlock(entry *IndexEntry, values *tsdb.IntegerArray) error {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return ErrTSMClosed
	}

	err := DecodeIntegerArrayBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	return err
}

func (m *mmapAccessor) readUnsignedBlock(entry *IndexEntry, values *[]UnsignedValue) ([]UnsignedValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeUnsignedBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readUnsignedArrayBlock(entry *IndexEntry, values *tsdb.UnsignedArray) error {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return ErrTSMClosed
	}

	err := DecodeUnsignedArrayBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	return err
}

func (m *mmapAccessor) readStringBlock(entry *IndexEntry, values *[]StringValue) ([]StringValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeStringBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readStringArrayBlock(entry *IndexEntry, values *tsdb.StringArray) error {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return ErrTSMClosed
	}

	err := DecodeStringArrayBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	return err
}

func (m *mmapAccessor) readBooleanBlock(entry *IndexEntry, values *[]BooleanValue) ([]BooleanValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeBooleanBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readBooleanArrayBlock(entry *IndexEntry, values *tsdb.BooleanArray) error {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return ErrTSMClosed
	}

	err := DecodeBooleanArrayBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	return err
}
