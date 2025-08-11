class IndexSnapshot:
    def __init__(self, index_id, index_value, percent_change, snapshot_date, total_advancing, total_declining):
        self.index_id = index_id
        self.index_value = index_value
        self.percent_change = percent_change
        self.snapshot_date = snapshot_date
        self.total_advancing = total_advancing
        self.total_declining = total_declining

class IndexSnapshotRepository:
    pass
