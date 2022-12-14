from the_bureaucrat.bureaucrats import RunBureaucrat # https://github.com/SengerM/the_bureaucrat
from pathlib import Path
import pandas
from huge_dataframe.SQLiteDataFrame import SQLiteDataFrameDumper, load_whole_dataframe, load_only_index_without_repeated_entries # https://github.com/SengerM/huge_dataframe
import sqlite3
from signals.PeakSignal import PeakSignal, draw_in_plotly # https://github.com/SengerM/signals
import numpy
import plotly.graph_objects as go

def parse_waveform(signal:PeakSignal)->dict:
	parsed = {
		'Amplitude (V)': signal.amplitude,
		'Noise (V)': signal.noise,
		'Rise time (s)': signal.rise_time,
		'Collected charge (V s)': signal.peak_integral,
		'Time over noise (s)': signal.time_over_noise,
		'Peak start time (s)': signal.peak_start_time,
		'Whole signal integral (V s)': signal.integral_from_baseline,
		'SNR': signal.SNR,
	}
	for threshold_percentage in [10,20,30,40,50,60,70,80,90]:
		try:
			time_over_threshold = signal.find_time_over_threshold(threshold_percentage)
		except Exception:
			time_over_threshold = float('NaN')
		parsed[f'Time over {threshold_percentage}% (s)'] = time_over_threshold
	for pp in [10,20,30,40,50,60,70,80,90]:
		try:
			time_at_this_pp = float(signal.find_time_at_rising_edge(pp))
		except Exception:
			time_at_this_pp = float('NaN')
		parsed[f't_{pp} (s)'] = time_at_this_pp
	return parsed

def plot_waveform(signal):
	fig = draw_in_plotly(signal)
	fig.update_layout(
		xaxis_title = "Time (s)",
		yaxis_title = "Amplitude (V)",
	)
	MARKERS = { # https://plotly.com/python/marker-style/#custom-marker-symbols
		10: 'circle',
		20: 'square',
		30: 'diamond',
		40: 'cross',
		50: 'x',
		60: 'star',
		70: 'hexagram',
		80: 'star-triangle-up',
		90: 'star-triangle-down',
	}
	for pp in [10,20,30,40,50,60,70,80,90]:
		try:
			fig.add_trace(
				go.Scatter(
					x = [signal.find_time_at_rising_edge(pp)],
					y = [signal(signal.find_time_at_rising_edge(pp))],
					mode = 'markers',
					name = f'Time at {pp} %',
					marker=dict(
						color = 'rgba(0,0,0,.5)',
						size = 11,
						symbol = MARKERS[pp]+'-open-dot',
						line = dict(
							color = 'rgba(0,0,0,.5)',
							width = 2,
						)
					),
				)
			)
		except Exception as e:
			pass
	return fig

def parse_waveforms(bureaucrat:RunBureaucrat, name_of_task_that_produced_the_waveforms_to_parse:str, continue_from_where_we_left_last_time:bool=True, silent:bool=True):
	Quique = bureaucrat
	
	Quique.check_these_tasks_were_run_successfully(name_of_task_that_produced_the_waveforms_to_parse)
	
	with Quique.handle_task('parse_waveforms', drop_old_data=not continue_from_where_we_left_last_time) as Quiques_employee:
		try:
			index_of_waveforms_already_parsed_in_the_past = set(load_whole_dataframe(Quiques_employee.path_to_directory_of_my_task/'parsed_from_waveforms.sqlite').index)
		except Exception:
			index_of_waveforms_already_parsed_in_the_past = set()
		
		path_to_waveforms_file = Quiques_employee.path_to_directory_of_task(name_of_task_that_produced_the_waveforms_to_parse)/'waveforms.sqlite'
		
		sqlite_connection = sqlite3.connect(path_to_waveforms_file)
		
		index_df = load_only_index_without_repeated_entries(path_to_waveforms_file)
		index_df = index_df.set_index(list(index_df.columns))
		index_of_waveforms_that_still_need_to_be_parsed = set(index_df.index) - index_of_waveforms_already_parsed_in_the_past
		index_of_waveforms_that_still_need_to_be_parsed = sorted(index_of_waveforms_that_still_need_to_be_parsed)
		if not silent:
			print(f'{len(index_of_waveforms_that_still_need_to_be_parsed)} waveforms still need to be parsed. The others were already parsed beforehand. Will now proceed...')
		
		with SQLiteDataFrameDumper(Quiques_employee.path_to_directory_of_my_task/Path('parsed_from_waveforms.sqlite'), dump_after_n_appends = 1111, dump_after_seconds = 60, delete_database_if_already_exists=False) as parsed_data_dumper: 
			for k,idx in enumerate(index_of_waveforms_that_still_need_to_be_parsed):
				sqlite_query = f'SELECT * from dataframe_table where ('
				index_to_select = ''
				for idx_val, idx_name in zip(idx, list(index_df.index.names)):
					index_to_select += f'{idx_name}=={idx_val}'
					index_to_select += ' and '
				index_to_select = index_to_select[:-5]
				sqlite_query += index_to_select + ')'
				if not silent:
					print(f'Processing {index_to_select} ({int(k/len(index_of_waveforms_that_still_need_to_be_parsed)*100)} %)...')
				waveform_df = pandas.read_sql_query(
					sqlite_query,
					sqlite_connection,
				)
				signal = PeakSignal(time=waveform_df['Time (s)'], samples=waveform_df['Amplitude (V)'])
				parsed_from_waveform = parse_waveform(signal)
				for idx_val, idx_name in zip(idx, list(index_df.index.names)):
					parsed_from_waveform[idx_name] = idx_val
				parsed_from_waveform_df = pandas.DataFrame(
					parsed_from_waveform,
					index = [0],
				).set_index(list(index_df.index.names), drop=True)
				parsed_data_dumper.append(parsed_from_waveform_df)
				
				if numpy.random.rand() < 20/len(index_df):
					if signal.SNR > 10 or numpy.random.rand() < .4:
						if not silent:
							print(f'Doing plot of this signal...')
						path_for_plots = Quiques_employee.path_to_directory_of_my_task/'plots_of_some_randomly_selected_waveforms'
						path_for_plots.mkdir(exist_ok=True)
						fig = plot_waveform(signal)
						fig.write_html(
							path_for_plots/f'{index_to_select.replace("==","_")}.html',
							include_plotlyjs = 'cdn',
						)

def parse_waveforms_recursively(bureaucrat:RunBureaucrat, name_of_task_that_produced_the_waveforms_to_parse:str, continue_from_where_we_left_last_time:bool=True, silent:bool=True):
	if bureaucrat.was_task_run_successfully(name_of_task_that_produced_the_waveforms_to_parse):
		if not silent:
			print(f'Going to parse {bureaucrat.run_name}...')
		parse_waveforms(
			bureaucrat = bureaucrat,
			name_of_task_that_produced_the_waveforms_to_parse = name_of_task_that_produced_the_waveforms_to_parse,
			continue_from_where_we_left_last_time = continue_from_where_we_left_last_time,
			silent = silent,
		)
	else:
		for path_to_task in bureaucrat.path_to_run_directory.iterdir():
			if path_to_task.is_dir():
				for subrun in bureaucrat.list_subruns_of_task(path_to_task.parts[-1]):
					parse_waveforms_recursively(
						bureaucrat = subrun,
						name_of_task_that_produced_the_waveforms_to_parse = name_of_task_that_produced_the_waveforms_to_parse,
						continue_from_where_we_left_last_time = continue_from_where_we_left_last_time,
						silent = silent,
					)

if __name__=='__main__':
	import argparse

	parser = argparse.ArgumentParser(description='Cleans a beta scan according to some criterion.')
	parser.add_argument('--dir',
		metavar = 'path',
		help = 'Path to the base measurement directory.',
		required = True,
		dest = 'directory',
		type = str,
	)

	args = parser.parse_args()
	parse_waveforms_recursively(
		bureaucrat = RunBureaucrat(Path(args.directory)),
		name_of_task_that_produced_the_waveforms_to_parse = 'test_beam',
		silent = False,
		continue_from_where_we_left_last_time = True,
	)
