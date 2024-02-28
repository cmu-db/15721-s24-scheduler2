from graphviz import Digraph

# Create a new directed graph
dot = Digraph(comment='Database Scheduler Architecture')

dot.attr(rankdir='TB')

# Add nodes with their respective labels
dot.node('Optimizer', 'Optimizer')
dot.node('Parse', 'Parse')
dot.node('QueryID Database', 'QueryID Database\n- QueryID\n- DAG\n- Leaves\n- Cost')
dot.node('FIFO Work Queue', 'FIFO Work Queue')
dot.node('Executor1', 'Executor 1')
dot.node('Executor2', 'Executor 2')
dot.node('Executor3', 'Executor 3')

dot.edge('Executor1', 'FIFO Work Queue', label='pull next task')
dot.edge('Executor2', 'FIFO Work Queue',  label='pull next task')
dot.edge('Executor3',  'FIFO Work Queue', label='pull next task')


# Add labels to some edges
dot.edge('Optimizer', 'Parse', label='Submits Datafusion ExecutionPlan')
dot.edge('Parse', 'QueryID Database', label='Add Entry')
dot.edge('QueryID Database', 'FIFO Work Queue', label='Enqueue Initial Work')
dot.edge('QueryID Database', 'FIFO Work Queue', label='Update Results', dir='back')

# Specify node styles
dot.attr('node', shape='box', style='filled', color='lightgrey', fontname="Helvetica")
dot.attr('node', shape='ellipse', style='filled', color='lightblue', fontname="Helvetica")

dot.render('./database_scheduler_architecture', format='png', view=False)