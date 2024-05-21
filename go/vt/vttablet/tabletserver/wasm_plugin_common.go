package tabletserver

import "unsafe"

const AbiVersion = "v1alpha1"

const WASMER = "wamser-go"
const WAZERO = "wazero"

func ptrToString(ptr uint32, size uint32) string {
	return unsafe.String((*byte)(unsafe.Pointer(uintptr(ptr))), size)
}

func stringToPtr(s string) (uint32, uint32) {
	ptr := unsafe.Pointer(unsafe.StringData(s))
	return uint32(uintptr(ptr)), uint32(len(s))
}
