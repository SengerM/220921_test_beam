from the_bureaucrat.bureaucrats import RunBureaucrat # https://github.com/SengerM/the_bureaucrat
from pathlib import Path
import pandas
import datetime
import time
from TheSetup import connect_me_with_the_setup
from huge_dataframe.SQLiteDataFrame import SQLiteDataFrameDumper, load_whole_dataframe # https://github.com/SengerM/huge_dataframe
import datetime
import threading
from parse_waveforms import parse_waveforms
from progressreporting.TelegramProgressReporter import TelegramReporter # https://github.com/SengerM/progressreporting
from contextlib import nullcontext, ExitStack
import my_telegram_bots
import numpy
import plotly.express as px

def trigger_and_measure_dut_stuff(the_setup, name_to_access_to_the_setup:str, slots_numbers:list)->pandas.DataFrame:
	elapsed_seconds = 9999
	while elapsed_seconds > 5: # Because of multiple threads locking the different elements of the_setup, it can happen that this gets blocked for a long time. Thus, the measured data will no longer belong to a single point in time as we expect...:
		while True:
			the_setup.wait_for_trigger(who=name_to_access_to_the_setup)
			try:
				# This is a check to be sure it will work.
				this_slot_data = the_setup.get_waveform(the_setup.get_slots_configuration_df().loc[slots_numbers[0],'oscilloscope_channel_number'])
			except RuntimeError as e:
				if 'The number of waveforms does not conincide with the number of segments.' in str(e):
					continue
				else:
					raise e
			break
		trigger_time = time.time()
		stuff = []
		for slot_number in slots_numbers:
			measured_stuff = {
				'Bias voltage (V)': the_setup.measure_bias_voltage(slot_number),
				'Bias current (A)': the_setup.measure_bias_current(slot_number),
				'device_name': the_setup.get_name_of_device_in_slot_number(slot_number),
				'slot_number': slot_number,
				'signal_name': the_setup.get_slots_configuration_df().loc[slot_number,'signal_name'],
				'When': datetime.datetime.now()
			}
			measured_stuff = pandas.DataFrame(measured_stuff, index=[0])
			stuff.append(measured_stuff)
		elapsed_seconds = trigger_time - time.time()
	return pandas.concat(stuff)

INDEX_COLUMNS = ['n_trigger','slot_number']

def test_beam(bureaucrat:RunBureaucrat, the_setup, name_to_access_to_the_setup:str, n_triggers:int, slots_numbers:list, silent:bool=True, reporter:TelegramReporter=None):
	report_progress = reporter is not None
	with bureaucrat.handle_task('test_beam') as employee, \
		the_setup.hold_signal_acquisition(name_to_access_to_the_setup), \
		SQLiteDataFrameDumper(
			employee.path_to_directory_of_my_task/'waveforms.sqlite',
			dump_after_n_appends = 1111,
			dump_after_seconds = 11,
		) as waveforms_dumper, \
		SQLiteDataFrameDumper(
			employee.path_to_directory_of_my_task/'extra_stuff.sqlite',
			dump_after_n_appends = 1111,
			dump_after_seconds = 11,
		) as extra_stuff_dumper, \
		reporter.report_for_loop(n_triggers, f'{bureaucrat.run_name}') if report_progress else nullcontext() as reporter \
	:
		with open(employee.path_to_directory_of_my_task/'setup_description.txt', 'w') as ofile:
			print(the_setup.get_description(), file=ofile)
			the_setup.get_slots_configuration_df().to_csv(employee.path_to_directory_of_my_task/'slots_configuration.csv')
		n_trigger = -1
		while n_trigger < n_triggers:
			n_trigger += 1
			
			if not silent:
				print(f'Waiting for trigger in the oscilloscope (n_trigger {n_trigger})...')
			measured_stuff = trigger_and_measure_dut_stuff(
				the_setup = the_setup,
				name_to_access_to_the_setup = name_to_access_to_the_setup,
				slots_numbers = slots_numbers,
			)
			measured_stuff['n_trigger'] = n_trigger
			extra_stuff_dumper.append(measured_stuff.set_index(INDEX_COLUMNS))
			
			if not silent:
				print(f'Acquiring n_trigger {n_trigger} out of {n_triggers}...')
			this_spill_data = []
			for slot_number in slots_numbers:
				this_slot_data = the_setup.get_waveform(the_setup.get_slots_configuration_df().loc[slot_number,'oscilloscope_channel_number'])
				this_n_trigger = n_trigger
				for i in range(len(this_slot_data)):
					if not silent:
						print(f'Processing n_trigger {this_n_trigger}/{n_triggers}, slot_number {slot_number}...')
					this_slot_data[i]['n_trigger'] = this_n_trigger
					this_slot_data[i]['slot_number'] = slot_number
					this_slot_data[i] = pandas.DataFrame(this_slot_data[i])
					this_slot_data[i] = this_slot_data[i].set_index(INDEX_COLUMNS)
					this_n_trigger += 1
				this_slot_data = pandas.concat(this_slot_data)
				this_spill_data.append(this_slot_data)
			this_spill_data = pandas.concat(this_spill_data)
			this_spill_data.sort_values(INDEX_COLUMNS, inplace=True)
			waveforms_dumper.append(this_spill_data)
			increment_n_trigger_by = this_n_trigger-1-n_trigger
			n_trigger += increment_n_trigger_by
			if report_progress:
				reporter.update(increment_n_trigger_by)
			if not silent:
				print(f'Finished acquiring n_trigger {n_trigger}.')

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

def acquire_and_parse(bureaucrat:RunBureaucrat, the_setup, name_to_access_to_the_setup:str, n_triggers:int, slots_numbers:list, delete_waveforms_file:bool, reporter:TelegramReporter=None, silent:bool=True):
	"""Perform a `TCT_1D_scan` and parse in parallel."""
	Ernestino = bureaucrat
	still_aquiring_data = True
	
	def parsing_thread_function():
		args = dict(
			bureaucrat = Ernestino, 
			name_of_task_that_produced_the_waveforms_to_parse = 'test_beam',
			silent = True, 
			continue_from_where_we_left_last_time = True,
		)
		while still_aquiring_data:
			try:
				parse_waveforms(**args)
			except:
				pass
			time.sleep(1)
		parse_waveforms(**args) # This last call is in case there is a bunch of waveforms left.
	
	parsing_thread = threading.Thread(target=parsing_thread_function)
	
	try:
		parsing_thread.start()
		test_beam(
			bureaucrat = Ernestino,
			the_setup = the_setup,
			name_to_access_to_the_setup = name_to_access_to_the_setup,
			slots_numbers = slots_numbers,
			n_triggers = n_triggers,
			reporter = reporter,
			silent = silent,
		)
	finally:
		still_aquiring_data = False
		while parsing_thread.is_alive():
			time.sleep(1)
		
		if delete_waveforms_file == True:
			(Ernestino.path_to_directory_of_task('acquire_test_beam_data')/'waveforms.sqlite').unlink()

def test_beam_sweeping_bias_voltage(bureaucrat:RunBureaucrat, the_setup, name_to_access_to_the_setup:str, n_triggers_per_voltage:int, slots_numbers:list, bias_voltages:dict, delete_waveforms_file:bool, reporter:TelegramReporter=None, silent:bool=True):
	if set(slots_numbers) != set(bias_voltages.keys()):
		raise ValueError(f'`bias_voltages` must be a dictionary whose keys are the same as the `slots_numbers`.')
	if any([len(bias_voltages[k])!=len(bias_voltages[list(bias_voltages.keys())[0]]) for k in bias_voltages.keys()]):
		raise ValueError(f'`bias_voltages` must be a dictionary of lists, all lists with the same lengths. If you dont want to sweep a voltage, just fill in a list with all identical values.')
	if not silent:
		print(f'Waiting to acquire control of hardware...')
	
	report_progress = reporter is not None
	with ExitStack() as stack, \
		bureaucrat.handle_task('test_beam_sweeping_bias_voltage') as employee, \
		reporter.report_for_loop(len(bias_voltages[slots_numbers[0]]), f'{bureaucrat.run_name}') if report_progress else nullcontext() as reporter \
	:
		for mgr in [the_setup.hold_control_of_bias_for_slot_number(slot_number=sn, who=name_to_access_to_the_setup) for sn in slots_numbers]:
			stack.enter_context(mgr)
		if not silent:
			print(f'Control of hardware acquired!')
		for i_voltage in range(len(bias_voltages[slots_numbers[0]])):
			for slot_number in slots_numbers:
				if not silent:
					print(f'Setting voltage {bias_voltages[slot_number][i_voltage]} to slot_number {slot_number}...')
				the_setup.set_bias_voltage(
					slot_number = slot_number,
					volts = bias_voltages[slot_number][i_voltage],
					who = name_to_access_to_the_setup,
				)
			acquire_and_parse(
				bureaucrat = employee.create_subrun(f'{bureaucrat.run_name}_i_voltage_{i_voltage}'),
				name_to_access_to_the_setup = NAME_TO_ACCESS_TO_THE_SETUP,
				n_triggers = n_triggers_per_voltage,
				the_setup = the_setup,
				slots_numbers = slots_numbers,
				delete_waveforms_file = delete_waveforms_file,
				silent = silent,
				reporter = TelegramReporter(
					telegram_token = my_telegram_bots.robobot.token, 
					telegram_chat_id = my_telegram_bots.chat_ids['Robobot TCT setup'],
				) if report_progress else None,
			)
			if report_progress:
				reporter.update(1)
	
if __name__=='__main__':
	import os
	from configuration_files.current_run import Alberto
	from utils import create_a_timestamp, interlace
	
	NAME_TO_ACCESS_TO_THE_SETUP = f'acquire test bean PID: {os.getpid()}'
	
	the_setup = connect_me_with_the_setup()
	
	VOLTAGES = interlace(numpy.linspace(0,280,22))
	
	with Alberto.handle_task('test_beam_data', drop_old_data=False) as employee:
		Mariano = employee.create_subrun(create_a_timestamp() + '_' + input('Measurement name? ').replace(' ','_'))
		test_beam_sweeping_bias_voltage(
			bureaucrat = Mariano,
			the_setup = the_setup,
			name_to_access_to_the_setup = NAME_TO_ACCESS_TO_THE_SETUP,
			n_triggers_per_voltage = 11111,
			slots_numbers = [1,2,3,4],
			bias_voltages = {
				1: VOLTAGES,
				2: VOLTAGES,
				3: VOLTAGES,
				4: [200]*len(VOLTAGES),
			},
			delete_waveforms_file = False,
			silent = False,
			reporter = TelegramReporter(
				telegram_token = my_telegram_bots.robobot.token, 
				telegram_chat_id = my_telegram_bots.chat_ids['Robobot TCT setup'],
			),
		)
		# ~ acquire_and_parse(
			# ~ bureaucrat = Mariano,
			# ~ the_setup = the_setup,
			# ~ name_to_access_to_the_setup = NAME_TO_ACCESS_TO_THE_SETUP,
			# ~ n_triggers = 222,
			# ~ slots_numbers = [1,2,3,4],
			# ~ delete_waveforms_file = False,
			# ~ silent = False,
			# ~ reporter = TelegramReporter(
				# ~ telegram_token = my_telegram_bots.robobot.token, 
				# ~ telegram_chat_id = my_telegram_bots.chat_ids['Robobot TCT setup'],
			# ~ ),
		# ~ )
