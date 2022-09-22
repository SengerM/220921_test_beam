import TeledyneLeCroyPy # https://github.com/SengerM/TeledyneLeCroyPy
from time import sleep
from CAENpy.CAENDesktopHighVoltagePowerSupply import CAENDesktopHighVoltagePowerSupply, OneCAENChannel # https://github.com/SengerM/CAENpy
from pathlib import Path
import pandas
from CrossProcessLock import CrossProcessNamedLock
from threading import RLock
from multiprocessing.managers import BaseManager

PATH_TO_CONFIGURATION_FILES_DIRECTORY = Path('/home/sengerm/scripts_and_codes/repos/220921_test_beam/configuration_files')

class TheRobocoldBetaSetup:
	"""This class wraps all the hardware so if there are changes it is 
	easy to adapt. It should be thread safe.
	"""
	def __init__(self, path_to_slots_configuration_file:Path=None):
		if path_to_slots_configuration_file is None:
			path_to_slots_configuration_file = Path('slots_configuration.csv')
		for name in {'path_to_slots_configuration_file'}:
			if not isinstance(locals()[name], Path):
				raise TypeError(f'`{name}` must be of type {type(Path())}, received object of type {type(locals()[name])}.')
		self.path_to_slots_configuration_file = path_to_slots_configuration_file
		
		self.slots_configuration_df # This will trigger the load of the file, so if it fails it does now.
		
		# Hardware elements ---
		self._oscilloscope = TeledyneLeCroyPy.LeCroyWaveRunner('USB0::0x05ff::0x1023::4751N40408::INSTR')
		self._caens = {
			'13398': CAENDesktopHighVoltagePowerSupply(port='/dev/ttyACM0'), # DT1470ET, the new one.
		}
		
		# Locks for hardware ---
		# These locks ensure that each hardware is accessed only once at
		# a time, but the user does not know anything about them.
		self._caen_Lock = RLock()
		self._oscilloscope_Lock = RLock()
		# Locks for the user to hold ---
		# These locks are so the user can hold the control of a part of
		# the setup for an extended period of time. I had to write my own
		# lock because the ones existing in Python are not compatible
		# with multiple processes.
		self._bias_for_slot_Lock = {slot_number: CrossProcessNamedLock(Path.home()) for slot_number in self.slots_configuration_df.index}
		self._signal_acquisition_Lock = CrossProcessNamedLock(Path.home())
	
	@property
	def description(self) -> str:
		"""Returns a string with a "human description" of the setup, i.e.
		which instruments are connected, etc. This is useful to print in
		a file when you measure something, then later on you know which 
		instruments were you using."""
		instruments = {
			'oscilloscope': 
				{
					'object': self._oscilloscope,
					'lock': self._oscilloscope_Lock,
				},
			'caen 1': 
				{
					'object': self._caens['13398'],
					'lock': self._caen_Lock,
				},
		}
		string =  'Instruments\n'
		string += '-----------\n\n'
		for instrument in instruments:
			with instruments[instrument]['lock']:
				string += f'{instruments[instrument]["object"].idn}\n'
		string += '\nSlots configuration\n'
		string += '-------------------\n\n'
		string += self.slots_configuration_df.to_string(max_rows=999,max_cols=999)
		return string
	
	@property
	def slots_configuration_df(self):
		"""Returns a data frame with the configuration as specified in
		the slots configuration file."""
		if not hasattr(self, '_slots_configuration_df'):
			self._slots_configuration_df = pandas.read_csv(
				self.path_to_slots_configuration_file,
				dtype = {
					'slot_number': int,
					'device_name': str,
					'caen_serial_number': str,
					'caen_channel_number': int,
					'oscilloscope_channel_number': int,
				},
				index_col = 'slot_number',
			)
		return self._slots_configuration_df.copy()
	
	# Bias voltage power supply ----------------------------------------
	
	def hold_control_of_bias_for_slot_number(self, slot_number:int, who:str):
		"""When this is called in a `with` statement, it will guarantee
		the exclusive control of the bias conditions for the slot. Note 
		that others will be able to measure, but not change the voltage/current.
		
		Parameters
		----------
		slot_number: int
			Number of the slot for which you want to hold the control of
			the bias.
		who: str
			A string identifying you. This can be whatever you want, but
			you have to use always the same. A good choice is `str(os.getpid())`
			because it will give all your imported modules the same name.
			This is a workaround because, surprisingly,  the Locks in python
			are not multiprocess friendly.
		
		Example
		-------
		```
		with the_setup.hold_control_of_bias_for_slot_number(slot_number, my_name):
			# Nobody else from other thread can change the bias conditions for this slot.
			the_setup.set_bias_voltage(slot_number, volts, my_name) # This will not change unless you change it here.
		```
		"""
		return self._bias_for_slot_Lock[slot_number](who)
	
	def is_bias_slot_number_being_hold_by_someone(self, slot_number:int):
		"""Returns `True` if anybody is holding the control of the bias
		for the given slot number. Otherwise, returns `False`."""
		return self._bias_for_slot_Lock[slot_number].locked()
	
	def measure_bias_voltage(self, slot_number:int)->float:
		"""Returns the measured bias voltage in the given slot.
		
		Parameters
		----------
		slot_number: int
			Number of the slot for which you want to hold the control of
			the bias.
		"""
		caen_channel = self._caen_channel_given_slot_number(slot_number)
		with self._caen_Lock:
			return caen_channel.V_mon
	
	def set_bias_voltage(self, slot_number:int, volts:float, who:str, block_until_not_ramping_anymore:bool=True):
		"""Set the bias voltage for the given slot.
		
		Parameters
		----------
		slot_number: int
			The number of the slot to which to set the bias voltage.
		volts: float
			The voltage to set.
		who: str
			A string identifying you. This can be whatever you want, but
			you have to use always the same. A good choice is `str(os.getpid())`
			because it will give all your imported modules the same name.
			This is a workaround because, surprisingly,  the Locks in python
			are not multiprocess friendly.
		freeze_until_not_ramping_anymore: bool, default True
			If `True`, the method will hold the execution frozen until the
			CAEN says it has stopped ramping the voltage. If `False`, returns
			immediately after setting the voltage. This function is "thread
			friendly" in the sense that it will not block the whole access
			to the CAEN power supplies while it waits for the ramping to
			stop. Yet it is thread safe.
		"""
		if not isinstance(volts, (int, float)):
			raise TypeError(f'`volts` must be a float number, received object of type {type(volts)}.')
		if not isinstance(block_until_not_ramping_anymore, bool):
			raise TypeError(f'`block_until_not_ramping_anymore` must be boolean.')
		with self._bias_for_slot_Lock[slot_number](who):
			caen_channel = self._caen_channel_given_slot_number(slot_number)
			with self._caen_Lock:
				caen_channel.V_set = volts
			if block_until_not_ramping_anymore:
				sleep(1) # It takes a while for the CAEN to realize that it has to change the voltage...
				while True:
					if self.is_ramping_bias_voltage(slot_number) == False:
						break
					sleep(1)
				sleep(3) # Empirically, after CAEN says it is not ramping anymore, you have to wait 3 seconds to be sure it actually stopped ramping...
	
	def is_ramping_bias_voltage(self, slot_number:int)->bool:
		caen_channel = self._caen_channel_given_slot_number(slot_number)
		with self._caen_Lock:
			return caen_channel.is_ramping
	
	def measure_bias_current(self, slot_number:int)->float:
		"""Measures the bias current for the given slot."""
		caen_channel = self._caen_channel_given_slot_number(slot_number)
		with self._caen_Lock:
			return caen_channel.I_mon
	
	def set_current_compliance(self, slot_number:int, amperes:float, who:str):
		"""Set the current compliance for the given slot."""
		if not isinstance(amperes, (int, float)):
			raise TypeError(f'`amperes` must be a float number, received object of type {type(amperes)}.')
		caen_channel = self._caen_channel_given_slot_number(slot_number)
		with self._bias_for_slot_Lock[slot_number](who), self._caen_Lock:
			caen_channel.set(
				PAR = 'ISET',
				VAL = 1e6*amperes,
			)
	
	def get_current_compliance(self, slot_number:int)->float:
		"""Returns the current compliance for the given slot number."""
		caen_channel = self._caen_channel_given_slot_number(slot_number)
		with self._caen_Lock:
			return caen_channel.get('ISET')
	
	def set_bias_voltage_status(self, slot_number:int, status:str, who:str):
		"""Turn on or off the bias voltage for the given slot.
		
		Parameters
		----------
		slot_number: int
			The number of the slot on which to operate.
		status: str
			Either `'on'` or `'off'`.
		who: str
			A string identifying you. This can be whatever you want, but
			you have to use always the same. A good choice is `str(os.getpid())`
			because it will give all your imported modules the same name.
			This is a workaround because, surprisingly,  the Locks in python
			are not multiprocess friendly.
		"""
		if status not in {'on','off'}:
			raise ValueError(f'`status` must be either "on" or "off", received {status}.')
		caen_channel = self._caen_channel_given_slot_number(slot_number)
		with self._bias_for_slot_Lock[slot_number](who), self._caen_Lock:
			caen_channel.output = status
	
	# Signal acquiring -------------------------------------------------
	
	def hold_signal_acquisition(self, who:str):
		"""When this is called in a `with` statement, it will guarantee
		the exclusive control of the signal acquisition system, i.e. the
		oscilloscope and the RF multiplexer (The Castle).
		
		who: str
			A string identifying you. This can be whatever you want, but
			you have to use always the same. A good choice is `str(os.getpid())`
			because it will give all your imported modules the same name.
			This is a workaround because, surprisingly,  the Locks in python
			are not multiprocess friendly.
		
		Example
		-------
		```
		with the_setup.hold_signal_acquisition(my_name):
			# Nobody else from other thread can change anything from the oscilloscope or The Castle.
		```
		"""
		return self._signal_acquisition_Lock(who)
	
	def set_oscilloscope_vdiv(self, oscilloscope_channel_number:int, vdiv:float, who:str):
		"""Set the vertical scale of the given channel in the oscilloscope."""
		with self._signal_acquisition_Lock(who), self._oscilloscope_Lock:
			self._oscilloscope.set_vdiv(channel=oscilloscope_channel_number, vdiv=vdiv)
			
	def set_oscilloscope_trigger_threshold(self, level:float, who:str):
		"""Set the threshold level of the trigger."""
		with self._signal_acquisition_Lock(who), self._oscilloscope_Lock:
			source = self._oscilloscope.get_trig_source()
			self._oscilloscope.set_trig_level(trig_source=source, level=level)
	
	def wait_for_trigger(self, who:str):
		"""Blocks execution until there is a trigger in the oscilloscope."""
		with self._signal_acquisition_Lock(who), self._oscilloscope_Lock:
			self._oscilloscope.wait_for_single_trigger()
	
	def get_waveform(self, oscilloscope_channel_number:int)->dict:
		"""Gets a waveform from the oscilloscope.
		
		Parameters
		----------
		oscilloscope_channel_number: int
			The number of channel you want to read from the oscilloscope.
		
		Returns
		-------
		waveform: dict
			A dictionary of the form `{'Time (s)': np.array, 'Amplitude (V)': np.array}`.
		"""
		with self._oscilloscope_Lock:
			return self._oscilloscope.get_waveform(channel=oscilloscope_channel_number)
	
	# Temperature and humidity sensor ----------------------------------
	
	def measure_temperature(self):
		"""Returns a reading of the temperature as a float number in Celsius."""
		return float('NaN')
	
	def measure_humidity(self):
		"""Returns a reading of the humidity as a float number in %RH."""
		return float('NaN')
	
	# Others -----------------------------------------------------------
	
	def get_name_of_device_in_slot_number(self, slot_number:int)->str:
		"""Get the name of the device in the given slot."""
		return self.slots_configuration_df.loc[slot_number,'device_name']
	
	def _caen_channel_given_slot_number(self, slot_number:int):
		caen_serial_number = self.slots_configuration_df.loc[slot_number,'caen_serial_number']
		caen_channel_number = int(self.slots_configuration_df.loc[slot_number,'caen_channel_number'])
		return OneCAENChannel(self._caens[caen_serial_number], caen_channel_number)
	
	def get_description(self)->str:
		"""Same as `.description` but without the property decorator,
		because the properties fail in multiprocess applications."""
		return self.description
	
	def get_oscilloscope_configuration_df(self)->pandas.DataFrame:
		"""Same as `.oscilloscope_configuration_df` but without the property decorator,
		because the properties fail in multiprocess applications."""
		return self.oscilloscope_configuration_df
	
	def get_slots_configuration_df(self)->pandas.DataFrame:
		"""Same as `.slots_configuration_df` but without the property decorator,
		because the properties fail in multiprocess applications."""
		return self.slots_configuration_df
	
def connect_me_with_the_setup():
	class TheSetup(BaseManager):
		pass

	TheSetup.register('get_the_setup')
	m = TheSetup(address=('', 50000), authkey=b'abracadabra')
	m.connect()
	the_setup = m.get_the_setup()
	return the_setup

def load_beta_scans_configuration()->pandas.DataFrame:
	return pandas.read_csv(PATH_TO_CONFIGURATION_FILES_DIRECTORY/'beta_scans_configuration.csv').set_index(['slot_number'])
	
if __name__=='__main__':
	from progressreporting.TelegramProgressReporter import TelegramReporter # https://github.com/SengerM/progressreporting
	import my_telegram_bots
	
	class TheSetupManager(BaseManager):
		pass
	
	reporter = TelegramReporter(
		telegram_token = my_telegram_bots.robobot.token,
		telegram_chat_id = my_telegram_bots.chat_ids['Long term tests setup'],
	)
	
	print('Opening the setup...')
	the_setup = TheRobocoldBetaSetup(
		path_to_slots_configuration_file = Path('configuration_files/slots_configuration.csv'),
	)
	
	TheSetupManager.register('get_the_setup', callable=lambda:the_setup)
	m = TheSetupManager(address=('', 50000), authkey=b'abracadabra')
	s = m.get_server()
	print('Ready!')
	try:
		s.serve_forever()
	except Exception as e:
		reporter.send_message(f'🔥 `TheRobocoldBetaSetup` crashed! Reason: `{repr(e)}`.')
