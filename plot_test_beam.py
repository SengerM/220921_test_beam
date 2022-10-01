from the_bureaucrat.bureaucrats import RunBureaucrat # https://github.com/SengerM/the_bureaucrat
from pathlib import Path
import pandas
import dominate # https://github.com/Knio/dominate
from huge_dataframe.SQLiteDataFrame import load_whole_dataframe # https://github.com/SengerM/huge_dataframe
import plotly.express as px

def plot_test_beam(bureaucrat:RunBureaucrat):
	bureaucrat.check_these_tasks_were_run_successfully(['test_beam','parse_waveforms'])
	
	with bureaucrat.handle_task('plot_test_beam') as employee:
		parsed_from_waveforms = load_whole_dataframe(bureaucrat.path_to_directory_of_task('parse_waveforms')/'parsed_from_waveforms.sqlite')
		extra_stuff = load_whole_dataframe(bureaucrat.path_to_directory_of_task('test_beam')/'extra_stuff.sqlite')
		
		variables_to_plot = set(parsed_from_waveforms.columns)
		
		signal_names = extra_stuff.reset_index(drop=False).set_index('slot_number')['signal_name']
		signal_names = signal_names[~signal_names.index.duplicated(keep='first')]
		data = parsed_from_waveforms.join(signal_names, on='slot_number')
		
		PATH_FOR_DISTRIBUTION_PLOTS = employee.path_to_directory_of_my_task/'distributions'
		PATH_FOR_DISTRIBUTION_PLOTS.mkdir(exist_ok=True)
		for variable in variables_to_plot:
			fig = px.ecdf(
				data.reset_index(drop=False),
				x = variable,
				color = 'signal_name',
				title = f'{variable} distribution<br><sup>Run: {bureaucrat.run_name}',
			)
			fig.write_html(
				PATH_FOR_DISTRIBUTION_PLOTS/f'{variable} ecdf.html',
				include_plotlyjs = 'cdn',
			)
		dimensions = set(variables_to_plot) - {f't_{i} (s)' for i in [10,20,30,40,60,70,80,90]} - {f'Time over {i}% (s)' for i in [10,30,40,50,60,70,80,90]} - {'device_name'} - {'signal_name'}
		fig = px.scatter_matrix(
			data.reset_index(drop=False),
			dimensions = sorted(dimensions),
			color = 'signal_name',
			title = f'Scatter matrix plot<br><sup>Run: {bureaucrat.run_name}',
		)
		fig.update_traces(diagonal_visible=False, showupperhalf=False, marker = {'size': 3})
		for k in range(len(fig.data)):
			fig.data[k].update(
				selected = dict(
					marker = dict(
						opacity = 1,
						color = 'black',
					)
				),
			)
		fig.write_html(
			employee.path_to_directory_of_my_task/'scatter_matrix_plot.html',
			include_plotlyjs = 'cdn',
		)

def plot_everything_from_test_beam_sweeping_bias_voltage(bureaucrat:RunBureaucrat, measured_stuff_vs_when:bool=False, all_distributions:bool=False):
	Ernesto = bureaucrat
	Ernesto.check_these_tasks_were_run_successfully('test_beam_sweeping_bias_voltage')
	
	with Ernesto.handle_task('plot_everything_from_test_beam_sweeping_bias_voltage') as Ernestos_employee:
		# ~ for b in Ernesto.list_subruns_of_task('test_beam_sweeping_bias_voltage'):
			# ~ plot_test_beam(bureaucrat=b)
		path_to_subplots = []
		for plot_type in {'Amplitude (V) ecdf','scatter_matrix_plot','Collected charge (V s) ecdf','t_50 (s) ecdf'}:
			for subrun in Ernestos_employee.list_subruns_of_task('test_beam_sweeping_bias_voltage'):
				if plot_type != 'scatter_matrix_plot':
					path_to_plot = Path('..')/(subrun.path_to_directory_of_task('plot_test_beam')/f'distributions/{plot_type}.html').relative_to(Ernesto.path_to_run_directory)
				else:
					path_to_plot = Path('..')/(subrun.path_to_directory_of_task('plot_test_beam')/f'{plot_type}.html').relative_to(Ernesto.path_to_run_directory)
				path_to_subplots.append(
					{
						'plot_type': plot_type,
						'path_to_plot': path_to_plot,
						'run_name': subrun.run_name,
					}
				)
		path_to_subplots_df = pandas.DataFrame(path_to_subplots).set_index('plot_type')
		for plot_type in set(path_to_subplots_df.index.get_level_values('plot_type')):
			document_title = f'{plot_type} plots from test beam sweeping bias voltage {Ernesto.run_name}'
			html_doc = dominate.document(title=document_title)
			with html_doc:
				dominate.tags.h1(document_title)
				if plot_type in {'scatter_matrix_plot'}: # This is because these kind of plots draw a lot of memory and will cause problems if they are loaded all together.
					with dominate.tags.ul():
						for idx,row in path_to_subplots_df.loc[plot_type].sort_values('run_name').iterrows():
							with dominate.tags.li():
								dominate.tags.a(row['run_name'], href=row['path_to_plot'])
				else:
					with dominate.tags.div(style='display: flex; flex-direction: column; width: 100%;'):
						for idx,row in path_to_subplots_df.loc[plot_type].sort_values('run_name').iterrows():
							dominate.tags.iframe(src=str(row['path_to_plot']), style=f'height: 100vh; min-height: 600px; width: 100%; min-width: 600px; border-style: none;')
			with open(Ernestos_employee.path_to_directory_of_my_task/f'{plot_type} together.html', 'w') as ofile:
				print(html_doc, file=ofile)

def script_core(bureaucrat:RunBureaucrat):
	if bureaucrat.was_task_run_successfully('test_beam'):
		plot_test_beam(bureaucrat=bureaucrat)
	elif bureaucrat.was_task_run_successfully('test_beam_sweeping_bias_voltage'):
		plot_everything_from_test_beam_sweeping_bias_voltage(bureaucrat = bureaucrat)
	else:
		raise RuntimeError(f'Dont know how to process run {repr(bureaucrat.run_name)} located in {bureaucrat.path_to_run_directory}.')

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
	
	Enrique = RunBureaucrat(Path(args.directory))
	script_core(
		bureaucrat = Enrique,
	)
