from the_bureaucrat.bureaucrats import RunBureaucrat # https://github.com/SengerM/the_bureaucrat
from pathlib import Path
import pandas
import datetime
import time
from TheSetup import connect_me_with_the_setup
from huge_dataframe.SQLiteDataFrame import SQLiteDataFrameDumper # https://github.com/SengerM/huge_dataframe
import datetime
import threading
from parse_waveforms import parse_waveforms

def trigger_and_measure_dut_stuff(the_setup, name_to_access_to_the_setup:str, slots_numbers:list)->pandas.DataFrame:
	elapsed_seconds = 9999
	while elapsed_seconds > 5: # Because of multiple threads locking the different elements of the_setup, it can happen that this gets blocked for a long time. Thus, the measured data will no longer belong to a single point in time as we expect...:
		the_setup.wait_for_trigger(who=name_to_access_to_the_setup)
		trigger_time = time.time()
		stuff = []
		for slot_number in slots_numbers:
			measured_stuff = {
				'Bias voltage (V)': the_setup.measure_bias_voltage(slot_number),
				'Bias current (A)': the_setup.measure_bias_current(slot_number),
				'device_name': the_setup.get_name_of_device_in_slot_number(slot_number),
				'slot_number': slot_number,
				'When': datetime.datetime.now()
			}
			measured_stuff = pandas.DataFrame(measured_stuff, index=[0])
			stuff.append(measured_stuff)
		elapsed_seconds = trigger_time - time.time()
	return pandas.concat(stuff)

INDEX_COLUMNS = ['n_trigger','slot_number']

def acquire_test_beam_data(bureaucrat:RunBureaucrat, the_setup, name_to_access_to_the_setup:str, n_triggers:int, slots_numbers:list, silent:bool=True):
	with bureaucrat.handle_task('acquire_test_beam_data') as employee, \
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
		) as extra_stuff_dumper \
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
			n_trigger = this_n_trigger-1
			if not silent:
				print(f'Finished acquiring n_trigger {n_trigger}.')

def acquire_and_parse(bureaucrat:RunBureaucrat, the_setup, name_to_access_to_the_setup:str, n_triggers:int, slots_numbers:list, delete_waveforms_file:bool, silent:bool=True):
	"""Perform a `TCT_1D_scan` and parse in parallel."""
	Ernestino = bureaucrat
	still_aquiring_data = True
	
	def parsing_thread_function():
		args = dict(
			bureaucrat = Ernestino, 
			name_of_task_that_produced_the_waveforms_to_parse = 'acquire_test_beam_data',
			silent = silent, 
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
		acquire_test_beam_data(
			bureaucrat = Ernestino,
			the_setup = the_setup,
			name_to_access_to_the_setup = name_to_access_to_the_setup,
			slots_numbers = slots_numbers,
			n_triggers = n_triggers,
			silent = silent,
		)
	finally:
		still_aquiring_data = False
		while parsing_thread.is_alive():
			time.sleep(1)
		
		if delete_waveforms_file == True:
			(Ernestino.path_to_directory_of_task('acquire_test_beam_data')/'waveforms.sqlite').unlink()
	
if __name__=='__main__':
	import os
	from configuration_files.current_run import Alberto
	from utils import create_a_timestamp
	
	SLOTS = [1,2,3,4]
	NAME_TO_ACCESS_TO_THE_SETUP = f'acquire test bean PID: {os.getpid()}'
	
	the_setup = connect_me_with_the_setup()
	
	with Alberto.handle_task('test_beam_data', drop_old_data=False) as employee:
		Mariano = employee.create_subrun(create_a_timestamp() + '_' + input('Measurement name? ').replace(' ','_'))
		acquire_test_beam_data(
			bureaucrat = Mariano,
			name_to_access_to_the_setup = NAME_TO_ACCESS_TO_THE_SETUP,
			n_triggers = 5555,
			the_setup = the_setup,
			slots_numbers = SLOTS,
			silent = False,
		)
		# ~ acquire_and_parse(
			# ~ bureaucrat = Mariano,
			# ~ the_setup = the_setup,
			# ~ name_to_access_to_the_setup = NAME_TO_ACCESS_TO_THE_SETUP,
			# ~ n_triggers = 555,
			# ~ slots_numbers = [1,2,3,4],
			# ~ delete_waveforms_file = False,
			# ~ silent = False,
		# ~ )
