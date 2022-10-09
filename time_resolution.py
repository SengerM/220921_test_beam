import pandas
from the_bureaucrat.bureaucrats import RunBureaucrat # https://github.com/SengerM/the_bureaucrat
from pathlib import Path
import plotly.express as px
import grafica.plotly_utils.utils # https://github.com/SengerM/grafica
from uncertainties import ufloat
from huge_dataframe.SQLiteDataFrame import load_whole_dataframe # https://github.com/SengerM/huge_dataframe
import pickle
from summarize_parameters import read_summarized_data

grafica.plotly_utils.utils.set_my_template_as_default()

def time_resolution_vs_bias_voltage_DUT_and_reference_trigger(bureaucrat:RunBureaucrat, signals_names:set, reference_signal_name:str, reference_signal_time_resolution:float, reference_signal_time_resolution_error:float):
	Norberto = bureaucrat
	
	Norberto.check_these_tasks_were_run_successfully(['jitter_calculation_test_beam_sweeping_voltage'])
	
	if reference_signal_name not in signals_names:
		raise ValueError(f'`reference_signal_name` is `{repr(reference_signal_name)}` which cannot be found in the measured signal names which are `{repr(signal_names)}`.')
	
	DUT_signal_name = signals_names - {reference_signal_name}
	if len(DUT_signal_name) != 1:
		raise RuntimeError(f'Cannot find the name of the DUT.')
	DUT_signal_name = list(DUT_signal_name)[0]
	
	with Norberto.handle_task('time_resolution_vs_bias_voltage_DUT_and_reference_trigger') as Norbertos_employee:
		jitter = pandas.read_pickle(Norberto.path_to_directory_of_task('jitter_calculation_test_beam_sweeping_voltage')/'jitter.pickle')
		
		jitter_was_calculated_between_these_signals = set(jitter.index.get_level_values('signals_names'))
		if len(jitter_was_calculated_between_these_signals) != 1:
			raise RuntimeError(f'Cannot use the jitter data, check this error!')
		jitter_was_calculated_between_these_signals = set(list(jitter_was_calculated_between_these_signals)[0])
		if jitter_was_calculated_between_these_signals != set(signals_names):
			raise RuntimeError(f'`signals_names` are {set(signals_names)} but signals in the calculation of the jitter are {jitter_was_calculated_between_these_signals}, they should match!')
		
		jitter['Jitter (s) ufloat'] = jitter.apply(lambda x: ufloat(x['Jitter (s)'],x['Jitter (s) error']), axis=1)
		
		reference_signal_time_resolution_ufloat = ufloat(reference_signal_time_resolution, reference_signal_time_resolution_error)
		
		DUT_time_resolution = (jitter['Jitter (s) ufloat']**2-reference_signal_time_resolution_ufloat**2)**.5
		DUT_time_resolution.rename(f'Time resolution (s) ufloat', inplace=True)
		DUT_time_resolution_df = DUT_time_resolution.to_frame()
		DUT_time_resolution_df[f'Time resolution (s)'] = DUT_time_resolution_df[f'Time resolution (s) ufloat'].apply(lambda x: x.nominal_value)
		DUT_time_resolution_df[f'Time resolution (s) error'] = DUT_time_resolution_df[f'Time resolution (s) ufloat'].apply(lambda x: x.std_dev)
		DUT_time_resolution_df.drop(columns=f'Time resolution (s) ufloat', inplace=True)
		DUT_time_resolution_df['signal_name'] = DUT_signal_name
		DUT_time_resolution_df.reset_index(level='signals_names', drop=True, inplace=True)
		
		reference_signal_time_resolution_df = pandas.DataFrame(
			{
				'Time resolution (s)': reference_signal_time_resolution,
				'Time resolution (s) error': reference_signal_time_resolution_error,
				'signal_name': reference_signal_name,
			},
			index = DUT_time_resolution_df.index,
		)
		for df in [DUT_time_resolution_df, reference_signal_time_resolution_df]:
			df.set_index('signal_name', append=True, inplace=True)
		
		time_resolution = pandas.concat([reference_signal_time_resolution_df, DUT_time_resolution_df])
		
		time_resolution.to_pickle(Norbertos_employee.path_to_directory_of_my_task/'time_resolution.pickle')
		
		summary = read_summarized_data(Norberto)
		summary.columns = [f'{col[0]} {col[1]}' for col in summary.columns]
		summary = summary.droplevel(level='slot_number')
		summary.reset_index(level='device_name',inplace=True,drop=False)
		
		df = time_resolution.join(summary)
		
		fig = px.line(
			df.reset_index(drop=False).sort_values(['device_name','Bias voltage (V) mean']),
			x = 'Bias voltage (V) mean',
			y = f'Time resolution (s)',
			error_x = 'Bias voltage (V) std',
			error_y = f'Time resolution (s) error',
			color = 'device_name',
			markers = True,
			title = f'Time resolution vs bias voltage<br><sup>Run: {Norberto.run_name}</sup>',
			hover_data = ['signal_name','Bias current (A) mean'],
			labels = {
				'Bias voltage (V) mean': 'Bias voltage (V)',
				'Bias current (A) mean': 'Bias current (A)',
			},
		)
		fig.update_layout(xaxis = dict(autorange = "reversed"))
		fig.write_html(
			str(Norbertos_employee.path_to_directory_of_my_task/'time_resolution_vs_bias_voltage.html'),
			include_plotlyjs = 'cdn',
		)

def time_resolution_vs_bias_voltage_twin_devices(bureaucrat:RunBureaucrat, signals_names:set):
	Norberto = bureaucrat
	
	Norberto.check_these_tasks_were_run_successfully(['jitter_calculation_test_beam_sweeping_voltage','test_beam_sweeping_bias_voltage'])
	
	with Norberto.handle_task('time_resolution_vs_bias_voltage_twin_devices') as Norbertos_employee:
		jitter = pandas.read_pickle(Norberto.path_to_directory_of_task('jitter_calculation_test_beam_sweeping_voltage')/'jitter.pickle')
		
		jitter_was_calculated_between_these_signals = set(jitter.index.get_level_values('signals_names'))
		if len(jitter_was_calculated_between_these_signals) != 1:
			raise RuntimeError(f'Cannot use the jitter data, check this error!')
		jitter_was_calculated_between_these_signals = set(list(jitter_was_calculated_between_these_signals)[0])
		if jitter_was_calculated_between_these_signals != set(signals_names):
			raise RuntimeError(f'`signals_names` are {set(signals_names)} but signals in the calculation of the jitter are {jitter_was_calculated_between_these_signals}, they should match!')
		
		jitter['Jitter (s) ufloat'] = jitter.apply(lambda x: ufloat(x['Jitter (s)'],x['Jitter (s) error']), axis=1)
		
		DUTs_time_resolution = []
		for signal_name in signals_names:
			DUT_time_resolution = jitter['Jitter (s) ufloat']/2**.5
			DUT_time_resolution.rename(f'Time resolution (s) ufloat', inplace=True)
			DUT_time_resolution_df = DUT_time_resolution.to_frame()
			DUT_time_resolution_df[f'Time resolution (s)'] = DUT_time_resolution_df[f'Time resolution (s) ufloat'].apply(lambda x: x.nominal_value)
			DUT_time_resolution_df[f'Time resolution (s) error'] = DUT_time_resolution_df[f'Time resolution (s) ufloat'].apply(lambda x: x.std_dev)
			DUT_time_resolution_df.drop(columns=f'Time resolution (s) ufloat', inplace=True)
			DUT_time_resolution_df['signal_name'] = signal_name
			DUT_time_resolution_df.set_index('signal_name',append=True,inplace=True)
			DUTs_time_resolution.append(DUT_time_resolution_df)
		DUTs_time_resolution = pandas.concat(DUTs_time_resolution)
		DUTs_time_resolution.reset_index(level='signals_names', drop=True, inplace=True)
		
		DUTs_time_resolution.to_pickle(Norbertos_employee.path_to_directory_of_my_task/'time_resolution.pickle')
		
		summarized_data = read_summarized_data(bureaucrat)
		summarized_data.columns = [f'{col[0]} {col[1]}' for col in summarized_data.columns]
		summarized_data.reset_index(level='device_name',inplace=True,drop=False)
		summarized_data.reset_index(level='slot_number',inplace=True,drop=True)
		
		df = DUTs_time_resolution.join(summarized_data, on=['signal_name','run_name'])
		fig = px.line(
			df.reset_index(drop=False).sort_values(['signal_name','Bias voltage (V) mean']),
			x = 'Bias voltage (V) mean',
			y = f'Time resolution (s)',
			error_x = 'Bias voltage (V) std',
			error_y = f'Time resolution (s) error',
			color = 'device_name',
			markers = True,
			title = f'Time resolution vs bias voltage (jitter/√2)<br><sup>Run: {Norberto.run_name}</sup>',
			hover_data = ['signal_name','Bias current (A) mean'],
			labels = {
				'Bias voltage (V) mean': 'Bias voltage (V)',
				'Bias current (A) mean': 'Bias current (A)',
			},
		)
		fig.update_traces(error_y = dict(width = 1, thickness = .8))
		fig.update_layout(xaxis = dict(autorange = "reversed"))
		fig.write_html(
			str(Norbertos_employee.path_to_directory_of_my_task/'time_resolution_vs_bias_voltage.html'),
			include_plotlyjs = 'cdn',
		)

def time_resolution_vs_bias_voltage_comparison(bureaucrat:RunBureaucrat):
	Nicanor = bureaucrat
	
	Nicanor.check_these_tasks_were_run_successfully('automatic_beta_scans')
	
	with Nicanor.handle_task('time_resolution_vs_bias_voltage_comparison') as Nicanors_employee:
		time_resolutions = []
		for Raúl in Nicanors_employee.list_subruns_of_task('automatic_beta_scans'):
			Raúl.check_these_tasks_were_run_successfully('time_resolution_vs_bias_voltage_DUT_and_reference_trigger')
			submeasurement_time_resolution = pandas.read_csv(Raúl.path_to_directory_of_task('time_resolution_vs_bias_voltage_DUT_and_reference_trigger')/'time_resolution.csv')
			submeasurement_time_resolution['beta_scan_vs_bias_voltage'] = Raúl.run_name
			time_resolutions.append(submeasurement_time_resolution)
		df = pandas.concat(time_resolutions, ignore_index=True)
		
		df.to_csv(Nicanors_employee.path_to_directory_of_my_task/'time_resolution.csv', index=False)
		
		df['measurement_timestamp'] = df['beta_scan_vs_bias_voltage'].apply(lambda x: x.split('_')[0])
		fig = px.line(
			df.sort_values(['measurement_timestamp','Bias voltage (V)','signal_name']),
			x = 'Bias voltage (V)',
			y = 'Time resolution (s)',
			error_y = 'Time resolution (s) error',
			color = 'measurement_timestamp',
			facet_col = 'signal_name',
			markers = True,
			title = f'Time resolution comparison<br><sup>Run: {Nicanor.run_name}</sup>',
			hover_data = ['beta_scan_vs_bias_voltage','measurement_name'],
			labels = {
				'measurement_name': 'Beta scan',
				'beta_scan_vs_bias_voltage': 'Beta scan vs bias voltage',
				'measurement_timestamp': 'Measurement timestamp',
			}
		)
		fig.write_html(
			str(Nicanors_employee.path_to_directory_of_my_task/'time_resolution_vs_bias_voltage_comparison.html'),
			include_plotlyjs = 'cdn',
		)

def script_core(bureaucrat:RunBureaucrat):
	REFERENCE_SIGNAL_TIME_RESOLUTION = 17.32e-12 # My best characterization of the Photonis PMT.
	REFERENCE_SIGNAL_TIME_RESOLUTION_ERROR = 2.16e-12 # My best characterization of the Photonis PMT.
	REFERENCE_SIGNAL_NAME = 'MCP-PMT'
	
	Manuel = bureaucrat
	if Manuel.was_task_run_successfully('test_beam_sweeping_bias_voltage'):
		time_resolution_vs_bias_voltage_twin_devices(bureaucrat = Manuel)
	elif Manuel.was_task_run_successfully('automatic_beta_scans'):
		for b in Manuel.list_subruns_of_task('automatic_beta_scans'):
			time_resolution_vs_bias_voltage_DUT_and_reference_trigger(
				bureaucrat = b,
				reference_signal_name = REFERENCE_SIGNAL_NAME,
				reference_signal_time_resolution = REFERENCE_SIGNAL_TIME_RESOLUTION,
				reference_signal_time_resolution_error = REFERENCE_SIGNAL_TIME_RESOLUTION_ERROR,
			)
		time_resolution_vs_bias_voltage_comparison(Manuel)
	else:
		raise RuntimeError(f'Dont know how to process run {repr(Manuel.run_name)} located in {Manuel.path_to_run_directory}.')

if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser()
	parser.add_argument('--dir',
		metavar = 'path',
		help = 'Path to the base measurement directory.',
		required = True,
		dest = 'directory',
		type = str,
	)

	args = parser.parse_args()
	Arnoldo = RunBureaucrat(Path(args.directory))
	
	time_resolution_vs_bias_voltage_twin_devices(
		bureaucrat = Arnoldo,
		signals_names = {'TI_1','TI_2'},
	)
	
	# ~ time_resolution_vs_bias_voltage_DUT_and_reference_trigger(
		# ~ bureaucrat = Arnoldo,
		# ~ signals_names = {'TI_B','TI_C'},
		# ~ reference_signal_name = 'TI_B',
		# ~ reference_signal_time_resolution = 33.75e-12,
		# ~ reference_signal_time_resolution_error = .98e-12,
	# ~ )
