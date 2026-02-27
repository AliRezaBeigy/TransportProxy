//! Hand-written FFI for slipstream-picoquic (same style as slipstream-rust).
//! Only the symbols needed for client connection and one bidirectional stream.

#![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]

use libc::{c_char, c_int, c_ulong, c_uint, c_void, size_t, sockaddr};

#[cfg(windows)]
use winapi::shared::ws2def::SOCKADDR_STORAGE as sockaddr_storage;
#[cfg(not(windows))]
use libc::sockaddr_storage;

pub const PICOQUIC_CONNECTION_ID_MAX_SIZE: usize = 20;
pub const PICOQUIC_RESET_SECRET_SIZE: usize = 16;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct picoquic_connection_id_t {
    pub id: [u8; PICOQUIC_CONNECTION_ID_MAX_SIZE],
    pub id_len: u8,
}

#[repr(C)]
pub struct picoquic_quic_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct picoquic_cnx_t {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum picoquic_call_back_event_t {
    picoquic_callback_stream_data = 0,
    picoquic_callback_stream_fin = 1,
    picoquic_callback_stream_reset = 2,
    picoquic_callback_stop_sending = 3,
    picoquic_callback_stateless_reset = 4,
    picoquic_callback_close = 5,
    picoquic_callback_application_close = 6,
    picoquic_callback_stream_gap = 7,
    picoquic_callback_prepare_to_send = 8,
    picoquic_callback_almost_ready = 9,
    picoquic_callback_ready = 10,
    picoquic_callback_datagram = 11,
    picoquic_callback_version_negotiation = 12,
    picoquic_callback_request_alpn_list = 13,
    picoquic_callback_set_alpn = 14,
    picoquic_callback_pacing_changed = 15,
    picoquic_callback_prepare_datagram = 16,
    picoquic_callback_datagram_acked = 17,
    picoquic_callback_datagram_lost = 18,
    picoquic_callback_datagram_spurious = 19,
    picoquic_callback_path_available = 20,
    picoquic_callback_path_suspended = 21,
    picoquic_callback_path_deleted = 22,
    picoquic_callback_path_quality_changed = 23,
    picoquic_callback_path_address_observed = 24,
    picoquic_callback_app_wakeup = 25,
}

pub type picoquic_stream_data_cb_fn = Option<
    unsafe extern "C" fn(
        cnx: *mut picoquic_cnx_t,
        stream_id: u64,
        bytes: *mut u8,
        length: size_t,
        fin_or_event: picoquic_call_back_event_t,
        callback_ctx: *mut c_void,
        stream_ctx: *mut c_void,
    ) -> c_int,
>;

pub type picoquic_connection_id_cb_fn = Option<
    unsafe extern "C" fn(
        quic: *mut picoquic_quic_t,
        cnx_id_local: picoquic_connection_id_t,
        cnx_id_remote: picoquic_connection_id_t,
        cnx_id_cb_data: *mut c_void,
        cnx_id_returned: *mut picoquic_connection_id_t,
    ),
>;

pub type picoquic_packet_loop_cb_fn = Option<
    unsafe extern "C" fn(
        quic: *mut picoquic_quic_t,
        cb_mode: c_int,
        callback_ctx: *mut c_void,
        callback_arg: *mut c_void,
    ) -> c_int,
>;

extern "C" {
    pub static picoquic_null_connection_id: picoquic_connection_id_t;

    /// Initialize TLS backend (OpenSSL etc.) on the calling thread. Call from main/runtime thread before spawning server thread so cert/key loading works on Windows.
    pub fn picoquic_tls_api_init();

    pub fn picoquic_current_time() -> u64;
    pub fn picoquic_create(
        max_nb_connections: c_uint,
        cert_file_name: *const c_char,
        key_file_name: *const c_char,
        cert_root_file_name: *const c_char,
        default_alpn: *const c_char,
        default_callback_fn: picoquic_stream_data_cb_fn,
        default_callback_ctx: *mut c_void,
        cnx_id_callback: picoquic_connection_id_cb_fn,
        cnx_id_callback_data: *mut c_void,
        reset_seed: *const u8,
        current_time: u64,
        p_simulated_time: *mut u64,
        ticket_file_name: *const c_char,
        ticket_encryption_key: *const u8,
        ticket_encryption_key_length: size_t,
    ) -> *mut picoquic_quic_t;

    pub fn picoquic_free(quic: *mut picoquic_quic_t);
    pub fn picoquic_set_cookie_mode(quic: *mut picoquic_quic_t, cookie_mode: c_int);

    pub fn picoquic_get_server_address(
        ip_address_text: *const c_char,
        server_port: c_int,
        server_address: *mut sockaddr_storage,
        is_name: *mut c_int,
    ) -> c_int;

    pub fn picoquic_create_cnx(
        quic: *mut picoquic_quic_t,
        initial_cnx_id: picoquic_connection_id_t,
        remote_cnx_id: picoquic_connection_id_t,
        addr_to: *const sockaddr,
        start_time: u64,
        preferred_version: c_uint,
        sni: *const c_char,
        alpn: *const c_char,
        client_mode: c_char,
    ) -> *mut picoquic_cnx_t;

    pub fn picoquic_start_client_cnx(cnx: *mut picoquic_cnx_t) -> c_int;
    pub fn picoquic_set_callback(
        cnx: *mut picoquic_cnx_t,
        callback_fn: picoquic_stream_data_cb_fn,
        callback_ctx: *mut c_void,
    );
    pub fn picoquic_close(cnx: *mut picoquic_cnx_t, application_reason_code: u64) -> c_int;

    pub fn picoquic_get_next_local_stream_id(cnx: *mut picoquic_cnx_t, is_unidir: c_int) -> u64;
    pub fn picoquic_mark_active_stream(
        cnx: *mut picoquic_cnx_t,
        stream_id: u64,
        is_active: c_int,
        v_stream_ctx: *mut c_void,
    ) -> c_int;
    pub fn picoquic_provide_stream_data_buffer(
        context: *mut c_void,
        nb_bytes: size_t,
        is_fin: c_int,
        is_still_active: c_int,
    ) -> *mut u8;

    pub fn picoquic_packet_loop(
        quic: *mut picoquic_quic_t,
        local_port: c_int,
        local_af: c_int,
        dest_if: c_int,
        socket_buffer_size: c_int,
        do_not_use_gso: c_int,
        loop_callback: picoquic_packet_loop_cb_fn,
        loop_callback_ctx: *mut c_void,
    ) -> c_int;

    /// Queue data on a stream for sending. set_fin: 1 = close stream after this data.
    pub fn picoquic_add_to_stream(
        cnx: *mut picoquic_cnx_t,
        stream_id: u64,
        data: *const u8,
        length: size_t,
        set_fin: c_int,
    ) -> c_int;

    /// OpenSSL error string captured at the point of failure in picoquic_master_tlscontext.
    /// Returns "" (empty string) on success or if never called.
    pub fn picoquic_openssl_get_saved_error() -> *const c_char;
    /// Short label of the step that failed in picoquic_master_tlscontext,
    /// e.g. "cert_load", "key_load", "cipher_suite", "ok". Returns "" if never called.
    pub fn picoquic_get_last_tls_fail_step() -> *const c_char;
    /// Longer description of the failure including filenames and OpenSSL error.
    pub fn picoquic_get_tls_fail_detail() -> *const c_char;
}

/// Packet loop callback modes (picoquic_packet_loop_cb_enum).
pub const PICOQUIC_PACKET_LOOP_READY: c_int = 0;
pub const PICOQUIC_PACKET_LOOP_AFTER_RECEIVE: c_int = 2;
pub const PICOQUIC_PACKET_LOOP_AFTER_SEND: c_int = 3;
pub const PICOQUIC_PACKET_LOOP_TIME_CHECK: c_int = 5;
pub const PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP: i32 = 0x400 + 47;

/// Options passed to the loop on picoquic_packet_loop_ready; set do_time_check (bit 0) so we get time_check callbacks.
/// C layout: three unsigned int bitfields in one word.
#[repr(C)]
pub struct picoquic_packet_loop_options_t {
    pub _flags: c_uint, /* do_time_check=1, do_system_call_duration=2, provide_alt_port=4 */
}

/// Argument for picoquic_packet_loop_time_check; we can reduce delta_t so the loop wakes soon when we have pending sends.
#[repr(C)]
pub struct packet_loop_time_check_arg_t {
    pub current_time: u64,
    pub delta_t: i64,
}

/// Returns the OpenSSL error string captured by picoquic at the TLS context failure point.
/// Returns None if the last call succeeded or picoquic_master_tlscontext was never called.
pub fn get_openssl_last_error() -> Option<String> {
    unsafe {
        let saved = picoquic_openssl_get_saved_error();
        if !saved.is_null() {
            let s = std::ffi::CStr::from_ptr(saved).to_string_lossy().into_owned();
            if !s.trim().is_empty() {
                return Some(s);
            }
        }
        None
    }
}

/// Returns the detailed failure description from picoquic_master_tlscontext,
/// including filenames and OpenSSL error. Returns None on success or if never called.
pub fn get_tls_fail_detail() -> Option<String> {
    unsafe {
        let p = picoquic_get_tls_fail_detail();
        if p.is_null() {
            return None;
        }
        let s = std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned();
        if s.is_empty() || s == "success" {
            None
        } else {
            Some(s)
        }
    }
}
