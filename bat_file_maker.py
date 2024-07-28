def func(_start, _end, _parts):
    elements_per_part = _end // _parts
    print(elements_per_part)
    _remainder = _end % _parts
    print(_remainder)
    _current = _start
    with open('run.bat', 'w') as file:
        for part in range(1, _parts + 1):
            if part == _parts:
                start_id = _current
                end_id = _current + elements_per_part + _remainder
            else:
                start_id = _current
                end_id = _current + elements_per_part
            _current += elements_per_part + 1
            file.write(f'start python -m bc_registry {start_id} {end_id}\n')


func(_start=1, _end=19252, _parts=25)
