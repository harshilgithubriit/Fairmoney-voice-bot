import os
from dotenv import load_dotenv

load_dotenv()
import azure.cognitiveservices.speech as speechsdk

cfg = speechsdk.SpeechConfig(
    subscription=os.getenv("AZURE_SPEECH_KEY"),
    region=os.getenv("AZURE_SPEECH_REGION"),
)
cfg.set_speech_synthesis_output_format(speechsdk.SpeechSynthesisOutputFormat.Raw8Khz16BitMonoPcm)
synth = speechsdk.SpeechSynthesizer(speech_config=cfg, audio_config=None)

ssml = (
    '<speak version="1.0" xml:lang="en-NG">'
    '<voice name="en-NG-EzinneNeural">'
    'Hello, this is Fairmoney Bank calling. May I know your name?'
    '</voice></speak>'
)

r = synth.speak_ssml_async(ssml).get()
if r.reason == speechsdk.ResultReason.SynthesizingAudioCompleted:
    print(f"EN-NG VOICE OK - {len(r.audio_data)} bytes (Nigerian English female)")
else:
    d = r.cancellation_details
    print("VOICE FAILED:", d.reason, "-", d.error_details)