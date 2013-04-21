use 5.008;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
my @scripts = qw(
scripts/gd_ProcIncrementEcAcRequestId.ksh
scripts/gd_ProcIncrementOrderId.ksh
scripts/s4pm_acquire_data.pl
scripts/s4pm_add_runtime_EDGL1BCHANNEL.pl
scripts/s4pm_add_runtime_GdMODL1B.pl
scripts/s4pm_add_runtime_GdMOL3SS.pl
scripts/s4pm_airs_L0_check.pl
scripts/s4pm_algorithm_stats.pl
scripts/s4pm_allocate_disk.pl
scripts/s4pm_attitude_check.pl
scripts/s4pm_auto_request.pl
scripts/s4pm_bootstrap_dprep.pl
scripts/s4pm_check_n_spec.ksh
scripts/s4pm_check_size.pl
scripts/s4pm_checksum.pl
scripts/s4pm_clean_algorithm_output.pl
scripts/s4pm_compose_request.pl
scripts/s4pm_configurator.pl
scripts/s4pm_convert_all_old2new_selectdata.pl
scripts/s4pm_create_string.pl
scripts/s4pm_delete_expired_data.pl
scripts/s4pm_export.pl
scripts/s4pm_failed_find_data_handler.pl
scripts/s4pm_failed_order_handler.pl
scripts/s4pm_failed_pge_handler.pl
scripts/s4pm_failed_qc_handler.pl
scripts/s4pm_failed_service_handler.pl
scripts/s4pm_find_by_coverage.pl
scripts/s4pm_find_data.pl
scripts/s4pm_fresh_filter.pl
scripts/s4pm_gbad_quality.pl
scripts/s4pm_gdas_0zf_check.pl
scripts/s4pm_generate_find_data_report.pl
scripts/s4pm_insert_datapool.pl
scripts/s4pm_is_hdf.pl
scripts/s4pm_is_night.pl
scripts/s4pm_l0.pl
scripts/s4pm_make_kit.pl
scripts/s4pm_nise_check.pl
scripts/s4pm_old2new_selectdatacfg.pl
scripts/s4pm_old2new_stringcfg.pl
scripts/s4pm_parse_rusage_from_ph.pl
scripts/s4pm_poll_data.pl
scripts/s4pm_prepare_run.pl
scripts/s4pm_prepare_run_resync.pl
scripts/s4pm_preselect_data.pl
scripts/s4pm_purge_bad_data.pl
scripts/s4pm_receive_dn.pl
scripts/s4pm_receive_pan.pl
scripts/s4pm_register_data.pl
scripts/s4pm_regular_block.pl
scripts/s4pm_remote_polling_pdr.pl
scripts/s4pm_reqif_day.pl
scripts/s4pm_reqif_night.pl
scripts/s4pm_request_data.pl
scripts/s4pm_request_subscription.pl
scripts/s4pm_reseq_l0_files.pl
scripts/s4pm_resubmit_data_request.pl
scripts/s4pm_resubmit_pdr.pl
scripts/s4pm_rollup_rusage.pl
scripts/s4pm_run_algorithm.pl
scripts/s4pm_run_easy.pl
scripts/s4pm_run_DCLI.ksh
scripts/s4pm_s4pa_subscription.pl
scripts/s4pm_sea_ice_check.pl
scripts/s4pm_select_data.pl
scripts/s4pm_ship_data.pl
scripts/s4pm_split_order.pl
scripts/s4pm_split_services.pl
scripts/s4pm_stage_for_pickup.pl
scripts/s4pm_stringmaker.pl
scripts/s4pm_stringmakerall.pl
scripts/s4pm_sub_notify.pl
scripts/s4pm_sweep_data.pl
scripts/s4pm_tk_admin.pl
scripts/s4pm_tk_algorithm_info.pl
scripts/s4pm_tk_auto_request.pl
scripts/s4pm_tk_compose_request.pl
scripts/s4pm_tk_compose_single_request.pl
scripts/s4pm_tk_delete_data.pl
scripts/s4pm_tk_disk_alloc.pl
scripts/s4pm_tk_install_algorithm.pl
scripts/s4pm_tk_max_children.pl
scripts/s4pm_tk_modify_pcf.pl
scripts/s4pm_tk_pwbox.pl
scripts/s4pm_tk_select_string.pl
scripts/s4pm_tk_trmon.pl
scripts/s4pm_tk_uninstall_algorithm.pl
scripts/s4pm_track_data.pl
scripts/s4pm_track_requests.pl
);
WriteMakefile(
    'NAME'              => 'S4PM',
    'EXE_FILES'         => \@scripts,
    'VERSION'           => '5.27.0',
    'PREREQ_PM'         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      ( AUTHOR     => 'GES DAAC') : ()),
);
