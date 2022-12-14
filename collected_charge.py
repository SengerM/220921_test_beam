from the_bureaucrat.bureaucrats import RunBureaucrat # https://github.com/SengerM/the_bureaucrat
from pathlib import Path
import pandas
import plotly.express as px
import plotly.graph_objects as go
import grafica.plotly_utils.utils # https://github.com/SengerM/grafica
import numpy
from huge_dataframe.SQLiteDataFrame import load_whole_dataframe # https://github.com/SengerM/huge_dataframe
from summarize_parameters import read_summarized_data

import sys 
sys.path.append(str(Path.home()/'scripts_and_codes/repos/robocold_beta_setup/analysis_scripts'))
from plot_beta_scan import binned_fit_langauss, hex_to_rgba

from clean_test_beam import tag_n_trigger_as_background_according_to_the_result_of_clean_test_beam
from landaupy import langauss, landau # https://github.com/SengerM/landaupy
from jitter_calculation import resample_measured_data
from grafica.plotly_utils.utils import scatter_histogram # https://github.com/SengerM/grafica
import warnings
import multiprocessing

N_BOOTSTRAP = 99
grafica.plotly_utils.utils.set_my_template_as_default()

def draw_langauss_fit(fig, popt, x_values:numpy.array, color:str, name:str, normalization_coefficient:float=1, **kwargs):
	fig.add_trace(
		go.Scatter(
			x = x_values,
			y = langauss.pdf(x_values, *popt)*normalization_coefficient,
			name = f'Langauss fit {name}<br>x<sub>MPV</sub>={popt[0]:.2e}<br>ξ={popt[1]:.2e}<br>σ={popt[2]:.2e}',
			line = dict(color = color, dash='dash'),
			legendgroup = name,
		)
	)
	fig.add_trace(
		go.Scatter(
			x = x_values,
			y = landau.pdf(x_values, popt[0], popt[1])*normalization_coefficient,
			name = f'Landau component {name}',
			line = dict(color = f'rgba{hex_to_rgba(color, .3)}', dash='dashdot'),
			legendgroup = name,
		)
	)

def collected_charge_in_test_beam(bureaucrat:RunBureaucrat, force:bool=False):
	Norberto = bureaucrat
	
	Norberto.check_these_tasks_were_run_successfully(['test_beam','parse_waveforms'])
	
	TASK_NAME = 'collected_charge_in_test_beam'
	
	if force == False and Norberto.was_task_run_successfully(TASK_NAME): # If this was already done, don't do it again...
		return
	
	with Norberto.handle_task(TASK_NAME) as task_handler:
		data_df = load_whole_dataframe(Norberto.path_to_directory_of_task('parse_waveforms')/'parsed_from_waveforms.sqlite')
		
		if set(data_df.index.names) != {'n_trigger','slot_number'}:
			raise ValueError(f'The data does not posses the correct index.')
		if 'Collected charge (V s)' not in data_df.columns:
			raise ValueError(f'The data does not posses information about the collected charge.')
		
		if Norberto.was_task_run_successfully('clean_test_beam'):
			data_df = tag_n_trigger_as_background_according_to_the_result_of_clean_test_beam(Norberto, data_df).query('is_background==False').drop(columns='is_background')
		
		slots_configuration = pandas.read_csv(Norberto.path_to_directory_of_task('test_beam')/'slots_configuration.csv').set_index('slot_number')
		
		data_df = data_df.join(slots_configuration['signal_name'], on='slot_number')
		data_df = data_df.droplevel(level='slot_number')
		data_df.set_index('signal_name',append=True,inplace=True)
		
		collected_charge_results = []
		for n_bootstrap in range(N_BOOTSTRAP):
			if n_bootstrap == 0:
				bootstrapped_iteration = False
			else:
				bootstrapped_iteration = True
			
			if not bootstrapped_iteration:
				df = data_df.copy()
			else: # if bootstrapped iteration
				df = resample_measured_data(data_df)
			df.index = df.index.droplevel('n_trigger')
			
			popts = {}
			bin_centerss = {}
			successful_fit = []
			for signal_name in set(df.index.get_level_values('signal_name')):
				successful_fit.append(False)
				try:
					popt, _, hist, bin_centers = binned_fit_langauss(df.loc[signal_name,'Collected charge (V s)'])
					successful_fit[-1] = True
					popts[signal_name] = popt # Need this to do the plot later on.
					bin_centerss[signal_name] =  bin_centers # Need this to do the plot later on.
				except Exception as e:
					pass
			
			if not all(successful_fit):
				if not bootstrapped_iteration:
					raise RuntimeError(f'Cannot fit a Langauss to the collected charge of one of the signals in run {Norberto.run_name}.')
				warnings.warn(f'Could not fit Langauss to one of the signals during a bootstrapped iteration. I will just try again...')
				n_bootstrap -= 1
				continue
			
			for signal_name in set(df.index.get_level_values('signal_name')):
				collected_charge_results.append(
					{
						'measured_on': 'real data' if bootstrapped_iteration == False else 'resampled data',
						'Collected charge (V s)': popts[signal_name][0],
						'signal_name': signal_name,
					}
				)
			
			if not bootstrapped_iteration:
				# Do some plotting...
				fig = go.Figure()
				fig.update_layout(
					title = f'Collected charge Langauss fit<br><sup>Run: {Norberto.run_name}</sup>',
					xaxis_title = 'Collected charge (V s)',
					yaxis_title = 'count',
				)
				colors = iter(px.colors.qualitative.Plotly)
				for signal_name in sorted(set(df.index.get_level_values('signal_name'))):
					samples = df.loc[signal_name,'Collected charge (V s)']
					color = next(colors)
					fig.add_trace(
						scatter_histogram(
							samples = samples,
							error_y = dict(type='auto'),
							density = False,
							name = f'Data {signal_name}',
							line = dict(color = color),
							legendgroup = signal_name,
						)
					)
					draw_langauss_fit(
						fig, 
						popt = popts[signal_name], 
						x_values = numpy.linspace(samples.min(),samples.max(),999), 
						color = color,
						name = signal_name,
						normalization_coefficient = len(samples)*numpy.diff(bin_centerss[signal_name])[0],
					)
				
				fig.write_html(
					str(task_handler.path_to_directory_of_my_task/'collected_charge_langauss_fit.html'),
					include_plotlyjs = 'cdn',
				)
		
		collected_charge_results_df = pandas.DataFrame(collected_charge_results).set_index(['signal_name','measured_on'])
		collected_charge_results_df.sort_index(inplace=True)
		collected_charge_final_results = []
		for signal_name in set(collected_charge_results_df.index.get_level_values('signal_name')):
			collected_charge_final_results.append(
				{
					'Collected charge (V s)': collected_charge_results_df.loc[(signal_name,'real data'),'Collected charge (V s)'][0],
					'Collected charge (V s) error': collected_charge_results_df.loc[(signal_name,'resampled data'),'Collected charge (V s)'].std(),
					'signal_name': signal_name,
				}
			)
		collected_charge_final_results_df = pandas.DataFrame(collected_charge_final_results).set_index('signal_name')
		collected_charge_final_results_df.to_pickle(task_handler.path_to_directory_of_my_task/'collected_charge.pickle')

def collected_charge_vs_bias_voltage(bureaucrat:RunBureaucrat, force_calculation_on_submeasurements:bool=False, number_of_processes:int=1):
	Romina = bureaucrat
	
	Romina.check_these_tasks_were_run_successfully('test_beam_sweeping_bias_voltage')
	
	subruns = Romina.list_subruns_of_task('test_beam_sweeping_bias_voltage')
	with multiprocessing.Pool(number_of_processes) as p:
		p.starmap(
			collected_charge_in_test_beam,
			[(bur,frc) for bur,frc in zip(subruns, [force_calculation_on_submeasurements]*len(subruns))]
		)
	
	with Romina.handle_task('collected_charge_vs_bias_voltage') as task_handler:
		charge = []
		for Raúl in subruns:
			Raúl.check_these_tasks_were_run_successfully('summarize_test_beam_extra_stuff')
			submeasurement_charge = pandas.read_pickle(Raúl.path_to_directory_of_task('collected_charge_in_test_beam')/'collected_charge.pickle')
			submeasurement_charge['run_name'] = Raúl.run_name
			submeasurement_charge.set_index('run_name', append=True, inplace=True)
			charge.append(submeasurement_charge)
		charge = pandas.concat(charge)
		
		charge.to_pickle(task_handler.path_to_directory_of_my_task/'collected_charge.pickle')
		
		summary = read_summarized_data(Romina)
		summary.columns = [f'{col[0]} {col[1]}' for col in summary.columns]
		
		fig = px.line(
			charge.join(summary).reset_index(drop=False).sort_values(['Bias voltage (V) mean','device_name']),
			x = 'Bias voltage (V) mean',
			y = 'Collected charge (V s)',
			error_y = 'Collected charge (V s) error',
			error_x = 'Bias voltage (V) std',
			color = 'device_name',
			title = f'Collected charge vs bias voltage<br><sup>Run: {Romina.run_name}</sup>',
			markers = True,
		)
		fig.update_layout(xaxis = dict(autorange = "reversed"))
		fig.write_html(
			str(task_handler.path_to_directory_of_my_task/'collected_charge_vs_bias_voltage.html'),
			include_plotlyjs = 'cdn',
		)
		
def script_core(bureaucrat:RunBureaucrat, force:bool):
	Manuel = bureaucrat
	if Manuel.was_task_run_successfully('test_beam_sweeping_bias_voltage'):
		collected_charge_vs_bias_voltage(
			bureaucrat = Manuel,
			force_calculation_on_submeasurements = force,
			number_of_processes = max(multiprocessing.cpu_count()-1,1)
		)
	elif Manuel.was_task_run_successfully('test_beam'):
		collected_charge_in_beta_scan(Manuel, force=True)
	else:
		raise RuntimeError(f'Dont know how to process run {repr(Manuel.run_name)} located in `{Manuel.path_to_run_directory}`...')

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
	parser.add_argument(
		'--force',
		help = 'If this flag is passed, it will force the calculation even if it was already done beforehand. Old data will be deleted.',
		required = False,
		dest = 'force',
		action = 'store_true'
	)

	args = parser.parse_args()
	bureaucrat = RunBureaucrat(Path(args.directory))
	script_core(
		bureaucrat = bureaucrat,
		force = args.force,
	)
