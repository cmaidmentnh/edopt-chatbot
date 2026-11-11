<?php
/**
 * Plugin Name: EdOpt Provider Audit (URL fixes)
 * Description: Signed REST endpoint used by the monthly provider audit (edopt-chatbot/provider_audit.py on 138.197.20.97) to update a provider's website / contact_page URL to its redirect target. Only those two ACF url fields, only published providers, only when the stored value still equals the expected old value. The old value is backed up to post meta _edopt_backup_<field>_<YYYYMMDD> before the change. Auth: HMAC-SHA256 over "<timestamp>.<raw body>" with the secret in wp_option edopt_audit_secret.
 * Version: 1.0
 * Author: EdOpt
 */

if (!defined('ABSPATH')) exit;

add_action('rest_api_init', function () {
    register_rest_route('edopt/v1', '/provider-url-fix', [
        'methods'             => 'POST',
        'callback'            => 'edopt_audit_url_fix',
        'permission_callback' => 'edopt_audit_verify_signature',
    ]);
});

function edopt_audit_verify_signature(WP_REST_Request $req) {
    $secret = get_option('edopt_audit_secret');
    if (!$secret || strlen($secret) < 32) return false;
    $ts  = (string) $req->get_header('x_edopt_timestamp');
    $sig = (string) $req->get_header('x_edopt_signature');
    if (!ctype_digit($ts) || abs(time() - (int) $ts) > 300 || !$sig) return false;
    $expected = hash_hmac('sha256', $ts . '.' . $req->get_body(), $secret);
    return hash_equals($expected, $sig);
}

function edopt_audit_url_fix(WP_REST_Request $req) {
    $data  = json_decode($req->get_body(), true);
    $fixes = is_array($data) && isset($data['fixes']) && is_array($data['fixes']) ? $data['fixes'] : [];
    $stamp = isset($data['backup_date']) && preg_match('/^\d{8}$/', $data['backup_date']) ? $data['backup_date'] : gmdate('Ymd');
    $dry   = !empty($data['dry_run']);
    if (count($fixes) > 50) return new WP_REST_Response(['error' => 'max 50 fixes per request'], 400);

    $results = [];
    $touched = [];
    foreach ($fixes as $fix) {
        $id    = isset($fix['post_id']) ? (int) $fix['post_id'] : 0;
        $field = isset($fix['field']) ? (string) $fix['field'] : '';
        $old   = isset($fix['old']) ? (string) $fix['old'] : '';
        $new   = isset($fix['new']) ? trim((string) $fix['new']) : '';
        $r = ['post_id' => $id, 'field' => $field, 'status' => 'skipped'];

        $post = $id ? get_post($id) : null;
        if (!$post || $post->post_type !== 'provider' || $post->post_status !== 'publish') {
            $r['reason'] = 'not a published provider';
        } elseif (!in_array($field, ['website', 'contact_page'], true)) {
            $r['reason'] = 'field not allowed';
        } elseif (!preg_match('#^https?://#i', $new) || !filter_var($new, FILTER_VALIDATE_URL) || strlen($new) > 500) {
            $r['reason'] = 'invalid new url';
        } else {
            $current = (string) get_post_meta($id, $field, true);
            if ($current !== $old) {
                $r['reason'] = 'current value differs from expected old value';
            } elseif ($current === $new) {
                $r['reason'] = 'already set';
            } elseif ($dry) {
                $r['status'] = 'would_update';
            } else {
                $bkey = '_edopt_backup_' . $field . '_' . $stamp;
                if (get_post_meta($id, $bkey, true) === '') {
                    update_post_meta($id, $bkey, $current);
                }
                update_field($field, $new, $id);
                $saved = (string) get_post_meta($id, $field, true);
                if ($saved === $new) {
                    $r['status'] = 'updated';
                    $r['backup_key'] = $bkey;
                    $touched[$id] = true;
                } else {
                    $r['status'] = 'error';
                    $r['reason'] = 'value did not persist';
                }
            }
        }
        $results[] = $r;
    }

    // Purge caches once per batch (not once per provider): Varnish by post ID, CF by URL in chunks of 30.
    if ($touched) {
        $urls = function_exists('edopt_provider_cache_urls') ? edopt_provider_cache_urls(0) : [];
        foreach (array_keys($touched) as $pid) {
            clean_post_cache($pid);
            if (class_exists('WpeCommon') && method_exists('WpeCommon', 'purge_varnish_cache')) {
                WpeCommon::purge_varnish_cache($pid, true);
            }
            $perma = get_permalink($pid);
            if ($perma) $urls[] = $perma;
        }
        if (class_exists('WpeCommon') && method_exists('WpeCommon', 'purge_memcached')) WpeCommon::purge_memcached();
        if (function_exists('edopt_provider_purge_cloudflare_urls')) {
            foreach (array_chunk(array_values(array_unique($urls)), 30) as $chunk) {
                edopt_provider_purge_cloudflare_urls($chunk);
            }
        }
    }

    return new WP_REST_Response(['results' => $results], 200);
}
