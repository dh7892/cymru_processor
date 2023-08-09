CREATE TYPE sink.DetectionType AS ENUM (
    'other',
    'netflow',
    'sinkhole',
    'darknet',
    'honeypot',
    'human_verified',
    'active_probe',
    'reported_by_3rd_party',
    'unverified_malware_c2'
);

CREATE TABLE sink.dave_team_cymru_repfeed (
    stamp timestamp without time zone,
    addr inet,
    notes text,
    cc text,
    rep_days_in_feed integer,
    rep_count_of_active_detections integer,
    rep_count_of_passive_detections integer,
    rep_detection_type sink.DetectionType,
    rep_ssl_usage bool,
    rep_controller_instruction_decoded bool,
    rep_ddos_command_observed bool,
    rep_non_standard_port bool,
    rep_number_of_unique_domain_names_on_same_ip integer,
    rep_number_of_distinct_controllers_on_same_ip integer,
    rep_other_bad_ips_in_24 integer,
    proto integer,
    family text,
    asn integer,
    category text,
    reputation_score integer,
    port integer
);