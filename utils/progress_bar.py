
from tqdm.auto import tqdm

class ProgressBar:
    """ class for handling progress bar status """
    def __init__(self):
        self.bar = None

    def init_progress_bar(self, max_len=None):

        if max_len is not None:
            self.bar = tqdm(total=max_len)
        else:
            self.bar = tqdm()

    def bar_set(self,text):
        """set text inside progress bar"""
        self.bar.set_description(text)

    def bar_update(self, new_len, new_text):
        """
        update inside descriptor of tqdm bar
        :param new_len: new len
        :param new_text: new text
        """
        self.bar.total = new_len
        self.bar.reset(total=new_len)
        self.bar.set_description(new_text)
        self.bar.refresh()